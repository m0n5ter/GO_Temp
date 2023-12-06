using GO.Workerservice.Config;
using GO.Workerservice.Mqtt;

namespace GO.Workerservice.Logic;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly MqttBroker _broker;
    private readonly MqttClient _client;
    private readonly IServiceProvider _serviceProvider;

    public Worker(IServiceProvider serviceProvider, ILogger<Worker> logger, MqttBroker broker, MqttClient client, Configuration configuration)
    {
        _serviceProvider = serviceProvider;
        _logger = logger;
        _broker = broker;
        _client = client;

        var displayPassword = configuration.DatabaseConfiguration.Password is null ? "-" : "***********";

        _logger.LogInformation("============== GO Workerservice is starting up ==============");

        _logger.LogInformation("DatabaseConfiguration:");
        _logger.LogInformation($"    - Host: {configuration.DatabaseConfiguration.Host}");
        _logger.LogInformation($"    - Database: {configuration.DatabaseConfiguration.Database}");
        _logger.LogInformation($"    - Engine: {configuration.DatabaseConfiguration.Engine}");
        _logger.LogInformation($"    - Username: {configuration.DatabaseConfiguration.Username}");
        _logger.LogInformation($"    - Password: {displayPassword}");
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await StartBroker();
        await _client.Connect(stoppingToken);
        _client.OnReceivingMessage += OnMessageReceived;

        while (!stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation("Worker running at: {time}", DateTime.Now);
            await Task.Delay(TimeSpan.FromMinutes(30), stoppingToken);
        }
    }

    private Task StartBroker() => _broker.RunBrokerAsync();

    private async void OnMessageReceived(object? sender, string e)
    {
        try
        {
            using var scope = _serviceProvider.CreateScope();
            var process = scope.ServiceProvider.GetRequiredService<Process>();
            _logger.LogInformation("New MQTT message received: {0}", e);
            await process.ProcessPackageAsync(e);
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Failed to process message");
        }
    }
}