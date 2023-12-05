namespace GO.Workerservice;

using System.Globalization;
using Microsoft.Extensions.Configuration;

public static class ConfigurationReader
{
    public static Configuration ReadConfiguration()
    {
        var builder = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json")
            .AddEnvironmentVariables()
            .Build();

        var configuration = builder.GetRequiredSection("Configuration").Get<Configuration>() ?? new Configuration();
        configuration.ReadExceptionFile();
        return configuration;
    }

    private static void ReadExceptionFile(this Configuration configuration)
    {
        using var sr = new StreamReader(configuration.ExceptionFilePath);

        while (sr.ReadLine() is { } line)
        {
            var parts = line.Split(';');
            var customerNr = parts[0];
            var volumeFactor = float.Parse(parts[1], CultureInfo.InvariantCulture.NumberFormat);
            configuration.ExceptionList.Add(parts[0], volumeFactor);
        }
    }
}