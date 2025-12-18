using Microsoft.Extensions.DependencyInjection;

namespace WhiteRabbit.Messaging.Abstractions;

internal class DefaultMessagingBuilder(IServiceCollection services) : IMessagingBuilder
{
    public IServiceCollection Services { get; } = services;
}
