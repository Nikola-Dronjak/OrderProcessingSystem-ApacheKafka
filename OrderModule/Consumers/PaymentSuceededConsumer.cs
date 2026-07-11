using Common.Events;
using Common.Messaging;
using Common.Messaging.Kafka;
using Confluent.Kafka;
using System.Text.Json;

namespace OrderModule.Consumers
{
    public class PaymentSuceededConsumer : KafkaConsumerBackgroundService
    {
        private const string GroupId = "order-group";

        private readonly IMessageBus messageBus;
        private readonly ILogger<PaymentSuceededConsumer> logger;

        public PaymentSuceededConsumer(IKafkaConsumerFactory consumerFactory, IMessageBus messageBus, ILogger<PaymentSuceededConsumer> logger) : base(consumerFactory, GroupId)
        {
            this.messageBus = messageBus;
            this.logger = logger;
        }

        protected override async Task ConsumeMessagesAsync(CancellationToken stoppingToken)
        {
            this.consumer.Subscribe(KafkaConstants.PaymentSucceededTopic);

            this.logger.LogInformation("PaymentSucceeded consumer started.");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    ConsumeResult<string, string> result = this.consumer.Consume(stoppingToken);
                    PaymentSucceededEvent? paymentSucceededEvent = JsonSerializer.Deserialize<PaymentSucceededEvent>(result.Message.Value);
                    if (paymentSucceededEvent == null)
                        continue;

                    this.logger.LogInformation("MESSAGE RECEIVED: {Message}", result.Message.Value);

                    OrderCompletedEvent orderCompletedEvent = new OrderCompletedEvent
                    {
                        OrderId = paymentSucceededEvent.OrderId,
                        Price = paymentSucceededEvent.Price,
                        PaymentId = paymentSucceededEvent.PaymentId,
                        CorrelationId = paymentSucceededEvent.CorrelationId,
                        Timestamp = DateTime.UtcNow
                    };

                    await this.messageBus.PublishAsync(
                        topic: KafkaConstants.OrderCompletedTopic,
                        message: orderCompletedEvent,
                        cancellationToken: stoppingToken);

                    this.logger.LogInformation(
                        """
                            ORDER COMPLETED
                            OrderId: {OrderId}
                            CorrelationId: {CorrelationId}
                            """,
                        orderCompletedEvent.OrderId,
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
