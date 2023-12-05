using MQTTnet.Client;
using MQTTnet.Packets;
using MQTTnet;
using System.Text;

namespace GO.Workerservice.Connection.Client;

public class MClient
{
    public event EventHandler<string>? OnReceivingMessage;
    public event EventHandler? ClientConnected;
    public event EventHandler? ClientDisconnected;

    public string BrokerIPAddress { get; private set; } = "localhost";
    
    public bool IsConnected { get; private set; }

    private readonly string _clientId;
    private readonly List<string> _topics;

    private readonly IConfiguration _configuration;
    private readonly ILogger<MClient> _logger;

    private readonly IMqttClient _client;
    private readonly MqttFactory _mqttFactory;

    public MClient(ILogger<MClient> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;

        _clientId = $"{Guid.NewGuid()}";

        _topics = new()
        {
            configuration["MessageTopic"] ?? "go-service"
        };

        _mqttFactory = new MqttFactory();
        _client = _mqttFactory.CreateMqttClient();
        _client.ConnectedAsync += Connected;
        _client.DisconnectedAsync += Disconnected;
        _client.ApplicationMessageReceivedAsync += MessageReceived;
    }

    private Task MessageReceived(MqttApplicationMessageReceivedEventArgs? msg) //TODO: Bestätigung ergänzen? => msg.AcknowledgeAsync()
    {
        var buffer = msg?.ApplicationMessage?.PayloadSegment.Array;

        if (buffer != null)
        {
            var packet = Encoding.UTF8.GetString(buffer);
            OnReceivingMessage?.Invoke(this, packet);
        }

        return Task.CompletedTask;
    }

    private Task Disconnected(MqttClientDisconnectedEventArgs arg)
    {
        if (arg.ClientWasConnected)
        {
            IsConnected = _client.IsConnected;
            ClientDisconnected?.Invoke(this, arg);
        }

        return Task.CompletedTask;
    }

    private Task Connected(MqttClientConnectedEventArgs arg)
    {
        _logger.LogInformation($"The client {_clientId} has been connected to broker: {BrokerIPAddress}:{_configuration["BrokerPort"]}");
        ClientConnected?.Invoke(this, EventArgs.Empty);

        IsConnected = _client.IsConnected;

        return Task.CompletedTask;
    }

    public async Task Connect(CancellationToken cancellationToken)
    {
        BrokerIPAddress = _configuration["BrokerIPAddress"] ?? "127.0.0.1";
        var options = GetOptions();
        await _client.ConnectAsync(options, cancellationToken);
        await SubscribeToTopics();
    }

    private MqttClientOptions GetOptions() => _mqttFactory.CreateClientOptionsBuilder()
        .WithClientId(_clientId)
        .WithTcpServer(BrokerIPAddress, int.Parse(_configuration["BrokerPort"] ?? "1883"))
        .Build();

    private async Task SubscribeToTopics() //TODO: to all topics from commondata / configuration
    {
        if (_topics.Count > 0)
        {
            var subscribeOptions = _mqttFactory.CreateSubscribeOptionsBuilder().Build();
            subscribeOptions.TopicFilters.AddRange(_topics.Select(topic => new MqttTopicFilter {Topic = topic}));
            await _client.SubscribeAsync(subscribeOptions);
        }
    }
}