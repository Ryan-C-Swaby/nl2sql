using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Spectre.Console;

var config = new ConfigurationBuilder()
    .AddUserSecrets<Program>()
    .AddEnvironmentVariables()
    .Build();

var AnthropicApiKey = config["ANTHROPIC_API_KEY"] ?? "";
var ConnectionString = config["MSSQL_CONNECTION_STRING"] ?? "";
const string Model = "claude-opus-4-5";

Console.Write("Table name: ");
var tableName = Console.ReadLine()?.Trim();
while (string.IsNullOrEmpty(tableName))
{
    AnsiConsole.MarkupLine("[red]Table name is required.[/]");
    Console.Write("Table name: ");
    tableName = Console.ReadLine()?.Trim();
}

Console.WriteLine("Fetching schema...");
var schema = await GetTableSchema(tableName);
if (schema == null)
{
    Console.WriteLine($"Table '{tableName}' not found or connection failed.");
    return;
}

Console.WriteLine($"Schema loaded ({schema.Split('\n').Length - 1} columns).\n");

using var httpClient = new HttpClient();
httpClient.DefaultRequestHeaders.Add("x-api-key", AnthropicApiKey);
httpClient.DefaultRequestHeaders.Add("anthropic-version", "2023-06-01");
httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

Console.WriteLine("NL2SQL Console — type a question or 'quit' to exit.\n");

while (true)
{
    Console.Write("Question: ");
    var input = Console.ReadLine()?.Trim();

    if (string.IsNullOrEmpty(input) || input.Equals("quit", StringComparison.OrdinalIgnoreCase))
        break;

    var sql = await TranslateToSql(input, schema);

    AnsiConsole.MarkupLine($"\n[bold yellow]Generated SQL:[/]");
    AnsiConsole.MarkupLine($"[cyan]{Markup.Escape(sql)}[/]\n");

    if (AnsiConsole.Confirm("Execute against database?"))
        await ExecuteAndDisplay(sql);

    Console.WriteLine();
}

async Task<string?> GetTableSchema(string table)
{
    try
    {
        await using var conn = new SqlConnection(ConnectionString);
        await conn.OpenAsync();

        const string existsQuery = """
            SELECT COUNT(1) FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = @table
            """;

        await using var existsCmd = new SqlCommand(existsQuery, conn);
        existsCmd.Parameters.AddWithValue("@table", table);
        var exists = (int)(await existsCmd.ExecuteScalarAsync())! > 0;
        if (!exists) return null;

        const string schemaQuery = """
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                CHARACTER_MAXIMUM_LENGTH,
                IS_NULLABLE,
                COLUMN_DEFAULT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = @table
            ORDER BY ORDINAL_POSITION
            """;

        await using var cmd = new SqlCommand(schemaQuery, conn);
        cmd.Parameters.AddWithValue("@table", table);

        var sb = new StringBuilder();
        sb.AppendLine($"Table: {table}");
        sb.AppendLine("Columns:");

        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var colName    = reader.GetString(0);
            var dataType   = reader.GetString(1);
            var maxLen     = reader.IsDBNull(2) ? "" : $"({reader.GetInt32(2)})";
            var nullable   = reader.GetString(3) == "YES" ? "NULL" : "NOT NULL";
            var colDefault = reader.IsDBNull(4) ? "" : $" DEFAULT {reader.GetString(4)}";

            sb.AppendLine($"  - {colName} {dataType.ToUpper()}{maxLen} {nullable}{colDefault}");
        }

        return sb.ToString();
    }
    catch (Exception ex)
    {
        Console.WriteLine($"DB error: {ex.Message}");
        return null;
    }
}

async Task ExecuteAndDisplay(string sql)
{
    try
    {
        await using var conn = new SqlConnection(ConnectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);

        await AnsiConsole.Status()
            .StartAsync("Running query...", async _ =>
            {
                await using var reader = await cmd.ExecuteReaderAsync();

                var table = new Table()
                    .Border(TableBorder.Rounded)
                    .BorderColor(Color.Grey);

                for (var i = 0; i < reader.FieldCount; i++)
                    table.AddColumn(new TableColumn($"[bold]{Markup.Escape(reader.GetName(i))}[/]"));

                var rowCount = 0;
                while (await reader.ReadAsync())
                {
                    var cells = new string[reader.FieldCount];
                    for (var i = 0; i < reader.FieldCount; i++)
                        cells[i] = reader.IsDBNull(i) ? "[grey]NULL[/]" : Markup.Escape(reader.GetValue(i).ToString() ?? "");
                    table.AddRow(cells);
                    rowCount++;
                }

                AnsiConsole.Write(table);
                AnsiConsole.MarkupLine($"[green]{rowCount} row(s) returned.[/]");
            });
    }
    catch (Exception ex)
    {
        AnsiConsole.MarkupLine($"[red]Query error:[/] {Markup.Escape(ex.Message)}");
    }
}

async Task<string> TranslateToSql(string question, string dbSchema)
{
    var systemPrompt = $"""
        You are an expert T-SQL query generator for Microsoft SQL Server.
        Given a natural language question and a database schema, return ONLY the SQL query — no explanation, no markdown fences.
        Use T-SQL syntax (TOP instead of LIMIT, GETDATE() for current date, etc.)

        {dbSchema}
        """;

    var requestBody = JsonSerializer.Serialize(new
    {
        model = Model,
        max_tokens = 1024,
        system = systemPrompt,
        messages = new[]
        {
            new { role = "user", content = question }
        }
    });

    var response = await httpClient.PostAsync(
        "https://api.anthropic.com/v1/messages",
        new StringContent(requestBody, Encoding.UTF8, "application/json"));

    response.EnsureSuccessStatusCode();

    var json = await response.Content.ReadAsStringAsync();
    using var doc = JsonDocument.Parse(json);

    var text = doc.RootElement
        .GetProperty("content")[0]
        .GetProperty("text")
        .GetString() ?? "(no response)";

    // Strip markdown code fences if the model returns them despite instructions
    text = System.Text.RegularExpressions.Regex.Replace(text, @"^```[a-zA-Z]*\n?", "", System.Text.RegularExpressions.RegexOptions.Multiline);
    text = text.Replace("```", "");

    return text.Trim();
}