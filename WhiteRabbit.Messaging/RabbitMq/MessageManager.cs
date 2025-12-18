using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using WhiteRabbit.Messaging.Abstractions;

namespace WhiteRabbit.Messaging.RabbitMq;

internal class MessageManager : IMessageSender, IDisposable
{
    private const string MaxPriorityHeader = "x-max-priority";

    internal IConnection Connection { get; private set; }
    internal IChannel Channel { get; private set; }

    private readonly MessageManagerSettings messageManagerSettings;
    private readonly QueueSettings queueSettings;

    public MessageManager(MessageManagerSettings messageManagerSettings, QueueSettings queueSettings)
    {
        var factory = new ConnectionFactory { Uri = new Uri(messageManagerSettings.ConnectionString) };

        Connection = factory.CreateConnectionAsync().GetAwaiter().GetResult();
        Channel = Connection.CreateChannelAsync().GetAwaiter().GetResult();

        if (messageManagerSettings.QueuePrefetchCount > 0)
        {
            Channel.BasicQosAsync(0, messageManagerSettings.QueuePrefetchCount, false);
        }

        Channel.ExchangeDeclareAsync(messageManagerSettings.ExchangeName, ExchangeType.Direct, durable: true);

        foreach (var (queue, args)
            in from (string Name, Type Type) queue in queueSettings.Queues
               let args = new Dictionary<string, object>
               {
                   [MaxPriorityHeader] = 10
               }
               select (queue, args))
        {
            Channel.QueueDeclareAsync(queue.Name, durable: true, exclusive: false, autoDelete: false, args);
            Channel.QueueBindAsync(queue.Name, messageManagerSettings.ExchangeName, queue.Name, null);
        }

        this.messageManagerSettings = messageManagerSettings;
        this.queueSettings = queueSettings;
    }

    public Task PublishAsync<T>(T message, int priority = 1) where T : class
    {
        var sendBytes = Encoding.UTF8.GetBytes(JsonSerializer.Serialize<object>(message, messageManagerSettings.JsonSerializerOptions ?? JsonOptions.Default));
        var routingKey = queueSettings.Queues.First(q => q.Type == typeof(T)).Name;

        return PublishAsync(sendBytes.AsMemory(), routingKey, priority);
    }

    private Task PublishAsync(ReadOnlyMemory<byte> body, string routingKey, int priority = 1)
    {
        var props = new BasicProperties
        {
            Persistent = true,
            Priority = Convert.ToByte(priority)
        };

        Channel.BasicPublishAsync(messageManagerSettings.ExchangeName, routingKey, true, props, body);

        return Task.CompletedTask;
    }

    public void MarkAsComplete(BasicDeliverEventArgs message) => Channel.BasicAckAsync(message.DeliveryTag, false);
    public void MarkAsRejected(BasicDeliverEventArgs message) => Channel.BasicRejectAsync(message.DeliveryTag, false);

    public void Dispose()
    {
        try
        {
            if (Channel.IsOpen)
            {
                Channel.CloseAsync().GetAwaiter().GetResult();
            }

            if (Connection.IsOpen)
            {
                Connection.CloseAsync().GetAwaiter().GetResult();
            }
        }
        catch
        {
            // Ignore exceptions on dispose
        }

        GC.SuppressFinalize(this);
    }
}