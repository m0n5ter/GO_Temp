namespace GO.Workerservice;

using Microsoft.Extensions.Logging;

public sealed class Configuration
{
    public LogLevel LogLevel { get; set; } = LogLevel.Information;

    public DatabaseConfiguration DatabaseConfiguration { get; set; } = null!;

    public string ScanLocation { get; set; } = "TST";

    public string ExceptionFilePath { get; set; } = "Exceptions.csv";

    public decimal DefaultVolumeFactor { get; set; } = 1.0m;

    public Dictionary<int, decimal> ExceptionList { get; set; } = new();
}