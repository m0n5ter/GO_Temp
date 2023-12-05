using GO.Workerservice.Connection.Broker;
using GO.Workerservice.Connection.Client;

namespace GO.Workerservice;

public class Program
{
    public static void Main(string[] args)
    {
        if (Environment.OSVersion.Platform == PlatformID.Win32NT)
        {
            var host = Host.CreateDefaultBuilder(args)
                .ConfigureServices(services =>
                {
                    services
                        .AddWindowsService(options => options.ServiceName = "GOService")
                        .AddSingleton(_ => ConfigurationReader.ReadConfiguration())
                        .AddSingleton<MBroker>()
                        .AddSingleton<MClient>()
                        .AddScoped<DatabaseService>()
                        .AddScoped<Process>()
                        .AddHostedService<Worker>();
                })
                .Build();

            host.Run();
        }
        else
        {
            Environment.Exit(0); //logging?
        }
    }
}