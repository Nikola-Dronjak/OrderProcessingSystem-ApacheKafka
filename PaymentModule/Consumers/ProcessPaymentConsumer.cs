using Common.Commands;
using Common.Events;
using Common.Messaging;
using Common.Messaging.Extensions;
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
        private readonly IConfiguration configuration;
        private readonly ILogger<ProcessPaymentConsumer> logger;

        public ProcessPaymentConsumer(IKafkaConsumerFactory consumerFactory, IConfiguration configuration, IMessageBus messageBus, ILogger<ProcessPaymentConsumer> logger) : base(consumerFactory, GroupId)
        {
            this.messageBus = messageBus;
            this.configuration = configuration;
            this.logger = logger;
        }

        protected override async Task ConsumeMessagesAsync(CancellationToken stoppingToken)
        {
            this.consumer.Subscribe(KafkaConstants.ProcessPaymentTopic);

            this.logger.LogInformation("Payment consumer started.");

            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string>? result = null;
                ProcessPaymentCommand? processPaymentCommand = null;

                try
                {
                    result = this.consumer.Consume(stoppingToken);
                    processPaymentCommand = JsonSerializer.Deserialize<ProcessPaymentCommand>(result.Message.Value);
                    if (processPaymentCommand == null)
                        continue;

                    this.logger.LogInformation("MESSAGE RECEIVED: {Message}", result.Message.Value);

                    // Simulate payment processing logic
                    await Task.Delay(SimulatedPaymentProcessingDelayMilliseconds, stoppingToken);

                    // Simulate random failure
                    int paymentFailurePercentage = this.configuration.GetValue<int>("PaymentProcessing:FailurePercentage");
                    if (Random.Shared.Next(1, 101) <= paymentFailurePercentage)
                        throw new Exception("Simulated payment processing failure.");

                    // Payment succeeded, publish success event
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
                    if (result == null || processPaymentCommand == null)
                    {
                        this.logger.LogError(ex, "Error processing payment.");
                        continue;
                    }

                    int retryCount = result.Message.Headers.GetRetryCount();
                    retryCount++;
                    Headers retryHeaders = KafkaHeaderExtensions.CreateRetryHeaders(retryCount);

                    this.logger.LogWarning(
                        ex,
                        """
                        PAYMENT FAILED
                        RetryCount: {RetryCount}
                        OrderId: {OrderId}
                        """,
                        retryCount,
                        processPaymentCommand.OrderId);

                    await this.messageBus.PublishAsync(
                        topic: KafkaConstants.PaymentFailedTopic,
                        message: processPaymentCommand,
                        headers: retryHeaders,
                        cancellationToken: stoppingToken);

                    this.logger.LogWarning(
                        """
                        MESSAGE SENT TO RETRY TOPIC
                        RetryAttempt: {RetryAttempt}
                        OrderId: {OrderId}
                        """,
                        retryCount,
                        processPaymentCommand.OrderId);
                }
            }
        }
    }
}