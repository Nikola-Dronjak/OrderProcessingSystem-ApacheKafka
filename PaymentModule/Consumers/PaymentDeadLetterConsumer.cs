using Common.Commands;
using Common.Events;
using Common.Messaging;
using Common.Messaging.Kafka;
using Confluent.Kafka;
using System.Text.Json;

namespace PaymentModule.Consumers
{
    public class PaymentDeadLetterConsumer : KafkaConsumerBackgroundService
    {
        private const string GroupId = "payment-dead-letter-group";

        private readonly IMessageBus messageBus;
        private readonly ILogger<PaymentDeadLetterConsumer> logger;

        public PaymentDeadLetterConsumer(IKafkaConsumerFactory consumerFactory, IMessageBus messageBus, ILogger<PaymentDeadLetterConsumer> logger) : base(consumerFactory, GroupId)
        {
            this.messageBus = messageBus;
            this.logger = logger;
        }

        protected override async Task ConsumeMessagesAsync(CancellationToken stoppingToken)
        {
            this.consumer.Subscribe(KafkaConstants.PaymentDeadLetterTopic);

            this.logger.LogInformation("PaymentDeadLetter consumer started.");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    ConsumeResult<string, string> result = this.consumer.Consume(stoppingToken);
                    ProcessPaymentCommand? processPaymentCommand = JsonSerializer.Deserialize<ProcessPaymentCommand>(result.Message.Value);
                    if (processPaymentCommand == null)
                        continue;

                    this.logger.LogInformation("MESSAGE RECEIVED: {Message}", result.Message.Value);

                    NotificationSentEvent notificationSentEvent = new NotificationSentEvent
                    {
                        OrderId = processPaymentCommand.OrderId,
                        IsSuccessful = false,
                        CorrelationId = processPaymentCommand.CorrelationId,
                        Timestamp = DateTime.UtcNow
                    };

                    await this.messageBus.PublishAsync(
                            topic: KafkaConstants.NotificationSentTopic,
                            message: notificationSentEvent,
                            cancellationToken: stoppingToken);
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
