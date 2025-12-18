using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using WhiteRabbit.Messaging.Abstractions;

namespace WhiteRabbit.Messaging.RabbitMq;

internal class MessageManager : IMessageSender, IAsyncDisposable
{
    private const string MaxPriorityHeader = "x-max-priority";

    internal IConnection Connection { get; private set; }
    internal IChannel Channel { get; private set; }

    private readonly MessageManagerSettings messageManagerSettings;
    private readonly QueueSettings queueSettings;

    private MessageManager(MessageManagerSettings messageManagerSettings, QueueSettings queueSettings)
    {
        this.messageManagerSettings = messageManagerSettings;
        this.queueSettings = queueSettings;
    }

    public static async Task<MessageManager> CreateAsync(MessageManagerSettings messageManagerSettings, QueueSettings queueSettings)
    {
        var factory = new ConnectionFactory { Uri = new Uri(messageManagerSettings.ConnectionString) };
        var manager = new MessageManager(messageManagerSettings, queueSettings)
        {
            Connection = await factory.CreateConnectionAsync().ConfigureAwait(false)
        };

        manager.Channel = await manager.Connection.CreateChannelAsync().ConfigureAwait(false);

        if (messageManagerSettings.QueuePrefetchCount > 0)
        {
            await manager.Channel.BasicQosAsync(0, messageManagerSettings.QueuePrefetchCount, false).ConfigureAwait(false);
        }

        await manager.Channel.ExchangeDeclareAsync(messageManagerSettings.ExchangeName, ExchangeType.Direct, durable: true).ConfigureAwait(false);

        foreach (var (queue, args)
            in from (string Name, Type Type) queue in queueSettings.Queues
               let args = new Dictionary<string, object>
               {
                   [MaxPriorityHeader] = 10
               }
               select (queue, args))
        {
            await manager.Channel.QueueDeclareAsync(queue.Name, durable: true, exclusive: false, autoDelete: false, args).ConfigureAwait(false);
            await manager.Channel.QueueBindAsync(queue.Name, messageManagerSettings.ExchangeName, queue.Name, null).ConfigureAwait(false);
        }

        return manager;
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

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (Channel?.IsOpen == true)
            {
                await Channel.CloseAsync().ConfigureAwait(false);
            }

            if (Connection?.IsOpen == true)
            {
                await Connection.CloseAsync().ConfigureAwait(false);
            }
        }
        catch
        {
            // Ignore exceptions on dispose
        }

        GC.SuppressFinalize(this);
    }
}