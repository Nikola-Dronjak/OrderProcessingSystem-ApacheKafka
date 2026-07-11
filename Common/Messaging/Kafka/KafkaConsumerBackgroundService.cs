using Confluent.Kafka;
using Microsoft.Extensions.Hosting;

namespace Common.Messaging.Kafka
{
    public abstract class KafkaConsumerBackgroundService : BackgroundService
    {
        protected readonly IConsumer<string, string> consumer;

        protected KafkaConsumerBackgroundService(IKafkaConsumerFactory consumerFactory, string groupId)
        {
            this.consumer = consumerFactory.CreateConsumer(groupId);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await Task.Yield();

            try
            {
                await this.ConsumeMessagesAsync(stoppingToken);
            }
            finally
            {
                this.consumer.Close();
                this.consumer.Dispose();
            }
        }

        protected abstract Task ConsumeMessagesAsync(CancellationToken stoppingToken);
    }
}
