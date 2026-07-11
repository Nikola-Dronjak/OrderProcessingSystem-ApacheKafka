using Confluent.Kafka;

namespace Common.Messaging.Kafka
{
    public interface IKafkaConsumerFactory
    {
        public IConsumer<string, string> CreateConsumer(string groupId);
    }
}
