using WhiteRabbit.Messaging.Abstractions;
using WhiteRabbit.Shared;

namespace WhiteRabbit.Receivers;

public class OrderMessageReceiver(ILogger<OrderMessageReceiver> logger, IMessageSender messageSender) : IMessageReceiver<Order>
{
    private readonly ILogger logger = logger;

    public async Task ReceiveAsync(Order message, CancellationToken cancellationToken)
    {
        logger.LogInformation("Processing order {OrderNumber}...", message.Number);

        await Task.Delay(TimeSpan.FromSeconds(10 + Random.Shared.Next(10)), cancellationToken);

        logger.LogInformation("End processing order {OrderNumber}", message.Number);

        await messageSender.PublishAsync(new Invoice { OrderNumber = message.Number });
    }
}