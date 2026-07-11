using Common.Commands;
using Common.Events;
using Common.Messaging;
using Common.Messaging.Kafka;
using Confluent.Kafka;
using System.Text.Json;

namespace PaymentModule.Consumers
{
    public class ProcessPaymentConsumer : KafkaConsumerBackgroundService
    {
        private const string GroupId = "payment-group";
        private const int SimulatedPaymentProcessingDelayMilliseconds = 2000;

        private readonly IMessageBus messageBus;
        private readonly ILogger<ProcessPaymentConsumer> logger;

        public ProcessPaymentConsumer(IKafkaConsumerFactory consumerFactory, IMessageBus messageBus, ILogger<ProcessPaymentConsumer> logger) : base(consumerFactory, GroupId)
        {
            this.messageBus = messageBus;
            this.logger = logger;
        }

        protected override async Task ConsumeMessagesAsync(CancellationToken stoppingToken)
        {
            this.consumer.Subscribe(KafkaConstants.ProcessPaymentTopic);

            this.logger.LogInformation("Payment consumer started.");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    ConsumeResult<string, string> result = this.consumer.Consume(stoppingToken);
                    ProcessPaymentCommand? processPaymentCommand = JsonSerializer.Deserialize<ProcessPaymentCommand>(result.Message.Value);
                    if (processPaymentCommand == null)
                        continue;

                    this.logger.LogInformation("MESSAGE RECEIVED: {Message}", result.Message.Value);

                    // Simulate payment processing logic
                    await Task.Delay(SimulatedPaymentProcessingDelayMilliseconds, stoppingToken);

                    PaymentSucceededEvent paymentSucceededEvent = new PaymentSucceededEvent
                    {
                        OrderId = processPaymentCommand.OrderId,
                        Price = processPaymentCommand.Price,
                        PaymentId = Guid.NewGuid(),
                        CorrelationId = processPaymentCommand.CorrelationId,
                        Timestamp = DateTime.UtcNow
                    };

                    await this.messageBus.PublishAsync(
                        topic: KafkaConstants.PaymentSucceededTopic,
                        message: paymentSucceededEvent,
                        cancellationToken: stoppingToken);

                    this.logger.LogInformation("Payment processed for order {OrderId}", processPaymentCommand.OrderId);
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
