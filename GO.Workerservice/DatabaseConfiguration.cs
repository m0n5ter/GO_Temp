namespace GO.Workerservice;

public sealed class DatabaseConfiguration
{
    public required string Host { get; set; } = null!;

    public int Port { get; set; } = 3306;

    public required string Username { get; set; } = null!;

    public string? Password { get; set; } = null;

    public required string Database { get; set; } = null!;

    public string ConnectionString => $"Server={Host};Port={Port};Database={Database};Uid={Username};Pwd={Password}";
}