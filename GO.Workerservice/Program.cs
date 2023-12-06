using GO.Workerservice.Config;
using GO.Workerservice.Data;
using GO.Workerservice.Logic;
using GO.Workerservice.Mqtt;
using Serilog;
using Serilog.Events;

namespace GO.Workerservice;

public class Program
{
    public static void Main(string[] args)
    {
        if (Environment.OSVersion.Platform == PlatformID.Win32NT)
        {
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Information()
                .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
                .Enrich.FromLogContext()
                .WriteTo.Console()
                .WriteTo.File(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Logs", "log_.log"), rollingInterval: RollingInterval.Day, retainedFileCountLimit: 90,
                    outputTemplate: "{Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} {CorrelationId} [{Level:u3}] ({SourceContext}) {Message:lj}{Exception}{NewLine}")
                .CreateLogger();

            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices(services =>
                {
                    services
                        .AddWindowsService(options => options.ServiceName = "GOService")
                        .AddSingleton(_ => ConfigurationReader.ReadConfiguration())
                        .AddSingleton<MqttBroker>()
                        .AddSingleton<MqttClient>()
                        .AddScoped<DatabaseService>()
                        .AddScoped<Process>()
                        .AddHostedService<Worker>();
                })
                .UseSerilog(Log.Logger)
                .Build();

            host.Run();
        }
        else
        {
            Environment.Exit(0);
        }
    }
}