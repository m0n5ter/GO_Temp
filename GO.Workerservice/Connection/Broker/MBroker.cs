using MQTTnet.Server;
using MQTTnet;

namespace GO.Workerservice.Connection.Broker;

public class MBroker
{
    private readonly MqttServer _broker;
    private readonly MqttFactory _mqttFactory;
    private readonly ILogger<MBroker> _logger;
    private readonly IConfiguration _configuration;

    public MBroker(ILogger<MBroker> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _mqttFactory = new MqttFactory();

        var options = GetOptions();

        _broker = _mqttFactory.CreateMqttServer(options);

        _broker.ClientConnectedAsync += ClientConnectedAsync;
        _broker.ClientDisconnectedAsync += ClientDisconnectedAsync;
    }

    private Task ClientDisconnectedAsync(ClientDisconnectedEventArgs e)
    {
        _logger.LogInformation($"The client: {e.ClientId} has been disconnected");
        return Task.CompletedTask;
    }

    private Task ClientConnectedAsync(ClientConnectedEventArgs e)
    {
        _logger.LogInformation($"The client: {e.ClientId} has been connected");
        return Task.CompletedTask;
    }

    private MqttServerOptions GetOptions() => _mqttFactory.CreateServerOptionsBuilder()
        .WithDefaultEndpoint()
        .WithDefaultEndpointPort(int.Parse(_configuration["BrokerPort"] ?? "1883"))
        .Build();

    public async Task RunBrokerAsync()
    {
        await _broker.StartAsync();
        _logger.LogInformation($"The broker has been started successfully");
    }
}