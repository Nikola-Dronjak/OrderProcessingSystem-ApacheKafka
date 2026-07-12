using Confluent.Kafka;

namespace Common.Messaging
{
    public interface IMessageBus
    {
        Task PublishAsync<T>(string topic, T message, Headers? headers = null, CancellationToken cancellationToken = default);
    }
}
