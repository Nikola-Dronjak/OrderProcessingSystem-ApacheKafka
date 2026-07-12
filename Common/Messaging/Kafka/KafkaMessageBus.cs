using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace Common.Messaging.Kafka
{
    public class KafkaMessageBus : IMessageBus
    {
        private readonly IProducer<string, string> producer;
        private readonly ILogger<KafkaMessageBus> logger;

        public KafkaMessageBus(IProducer<string, string> producer, ILogger<KafkaMessageBus> logger)
        {
            this.producer = producer;
            this.logger = logger;
        }

        public async Task PublishAsync<T>(string topic, T message, Headers? headers, CancellationToken cancellationToken)
        {
            string payload = JsonSerializer.Serialize(message);
            Message<string, string> kafkaMessage = new Message<string, string>
            {
                Key = Guid.NewGuid().ToString(),
                Value = payload,
                Headers = headers
            };

            await this.producer.ProduceAsync(
                topic: topic,
                message: kafkaMessage,
                cancellationToken: cancellationToken);

            this.logger.LogInformation(
                """
                MESSAGE PRODUCED
                Topic: {Topic}
                Key: {Key}
                Payload: {Payload}
                """,
                topic,
                kafkaMessage.Key,
                payload);
        }
    }
}
