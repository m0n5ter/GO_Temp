namespace GO.Workerservice;

public sealed class DatabaseConfiguration
{
    public required string Host { get; set; } = "192.168.103.201";

    public required string Username { get; set; } = "budde";

    public string? Password { get; set; } = null;

    public required string Database { get; set; } = "godus";

    public required string Engine { get; set; } = "test";

    public string ConnectionString => $@"Driver={{SQL Anywhere 10}};DatabaseName={Database};EngineName={Engine};uid={Username};pwd={Password};LINKs=tcpip(host={Host})";
}