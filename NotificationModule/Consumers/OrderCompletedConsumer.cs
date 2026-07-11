using Common.Events;
using Common.Messaging.Kafka;
using Confluent.Kafka;
using System.Text.Json;

namespace NotificationModule.Consumers
{
    public class OrderCompletedConsumer : KafkaConsumerBackgroundService
    {
        private const string GroupId = "notification-group";
        private const int SimulatedNotificationProcessingDelayMilliseconds = 500;

        private readonly ILogger<OrderCompletedConsumer> logger;

        public OrderCompletedConsumer(IKafkaConsumerFactory consumerFactory, ILogger<OrderCompletedConsumer> logger) : base(consumerFactory, GroupId)
        {
            this.logger = logger;
        }

        protected override async Task ConsumeMessagesAsync(CancellationToken stoppingToken)
        {
            this.consumer.Subscribe(KafkaConstants.PaymentSucceededTopic);

            this.logger.LogInformation("OrderCompleted consumer started.");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    ConsumeResult<string, string> result = this.consumer.Consume(stoppingToken);
                    OrderCompletedEvent? orderCompletedEvent = JsonSerializer.Deserialize<OrderCompletedEvent>(result.Message.Value);
                    if (orderCompletedEvent == null)
                        continue;

                    this.logger.LogInformation("MESSAGE RECEIVED: {Message}", result.Message.Value);

                    // Simulate notification sending logic
                    await Task.Delay(SimulatedNotificationProcessingDelayMilliseconds, stoppingToken);

                    this.logger.LogInformation(
                        """
                            NOTIFICATION SENT
                            OrderId: {OrderId}
                            Price: {Price}
                            PaymentId: {PaymentId}
                            CorrelationId: {CorrelationId}
                            """,
                        orderCompletedEvent.OrderId,
                        orderCompletedEvent.Price,
                        orderCompletedEvent.PaymentId,
                        orderCompletedEvent.CorrelationId);
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
