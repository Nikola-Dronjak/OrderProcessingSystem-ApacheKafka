using BenchmarkModule.Services;
using Common.Events;
using Common.Messaging.Kafka;
using Confluent.Kafka;
using System.Text.Json;

namespace BenchmarkModule.Consumers
{
    public class NotificationSentConsumer : KafkaConsumerBackgroundService
    {
        private const string GroupId = "benchmark-group";

        private readonly IMetricsCollectorService metricsCollectorService;
        private readonly ILogger<NotificationSentConsumer> logger;

        public NotificationSentConsumer(IKafkaConsumerFactory consumerFactory, IMetricsCollectorService metricsCollectorService, ILogger<NotificationSentConsumer> logger) : base(consumerFactory, GroupId)
        {
            this.metricsCollectorService = metricsCollectorService;
            this.logger = logger;
        }

        protected override async Task ConsumeMessagesAsync(CancellationToken stoppingToken)
        {
            this.consumer.Subscribe(KafkaConstants.NotificationSentTopic);

            this.logger.LogInformation("NotificationSent consumer started.");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    ConsumeResult<string, string> result = this.consumer.Consume(stoppingToken);
                    NotificationSentEvent? notificationSentEvent = JsonSerializer.Deserialize<NotificationSentEvent>(result.Message.Value);
                    if (notificationSentEvent == null)
                        continue;

                    this.logger.LogInformation("MESSAGE RECEIVED: {Message}", result.Message.Value);

                    this.metricsCollectorService.RegisterOrderCompletion(notificationSentEvent.OrderId, notificationSentEvent.IsSuccessful);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    this.logger.LogError(ex, "Error processing message");
                }
            }
        }
    }
}
