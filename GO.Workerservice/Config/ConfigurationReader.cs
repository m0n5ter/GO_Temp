using System.Globalization;

namespace GO.Workerservice.Config;

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
        if (!File.Exists(configuration.ExceptionFilePath)) return;

        foreach (var line in File.ReadAllLines(configuration.ExceptionFilePath))
        {
            var parts = line.Split(';');

            if (parts.Length > 1
                && int.TryParse(parts[0], out var customerNr)
                && decimal.TryParse(parts[1], CultureInfo.InvariantCulture.NumberFormat, out var volumeFactor))
                configuration.ExceptionList.Add(customerNr, volumeFactor);
        }
    }
}