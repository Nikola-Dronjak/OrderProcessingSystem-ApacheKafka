using Confluent.Kafka;
using Microsoft.Extensions.Hosting;

namespace Common.Messaging.Kafka
{
    public abstract class KafkaConsumerBackgroundService : BackgroundService
    {
        protected readonly IConsumer<string, string> consumer;

        protected KafkaConsumerBackgroundService(IConsumer<string, string> consumer)
        {
            this.consumer = consumer;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await Task.Yield();

            await this.ConsumeMessagesAsync(stoppingToken);
        }

        protected abstract Task ConsumeMessagesAsync(CancellationToken stoppingToken);
    }
}
