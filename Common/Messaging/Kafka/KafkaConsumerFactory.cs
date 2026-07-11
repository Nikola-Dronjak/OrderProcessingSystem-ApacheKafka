using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace Common.Messaging.Kafka
{
    public class KafkaConsumerFactory : IKafkaConsumerFactory
    {
        private readonly KafkaSettings settings;

        public KafkaConsumerFactory(IOptions<KafkaSettings> options)
        {
            settings = options.Value;
        }

        public IConsumer<string, string> CreateConsumer(string groupId)
        {
            ConsumerConfig config = new ConsumerConfig
            {
                BootstrapServers = settings.BootstrapServers,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true
            };

            return new ConsumerBuilder<string, string>(config).Build();
        }
    }
}
