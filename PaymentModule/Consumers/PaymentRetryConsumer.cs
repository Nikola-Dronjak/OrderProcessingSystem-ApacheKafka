using Common.Commands;
using Common.Messaging;
using Common.Messaging.Extensions;
using Common.Messaging.Kafka;
using Confluent.Kafka;
using System.Text.Json;

namespace PaymentModule.Consumers
{
    public class PaymentRetryConsumer : KafkaConsumerBackgroundService
    {
        private const string GroupId = "payment-retry-group";
        private const int RetryDelayMilliseconds = 5000;
        private const int MaxRetryCount = 3;

        private readonly IMessageBus messageBus;
        private readonly ILogger<PaymentRetryConsumer> logger;

        public PaymentRetryConsumer(IKafkaConsumerFactory consumerFactory, IMessageBus messageBus, ILogger<PaymentRetryConsumer> logger) : base(consumerFactory, GroupId)
        {
            this.messageBus = messageBus;
            this.logger = logger;
        }

        protected override async Task ConsumeMessagesAsync(CancellationToken stoppingToken)
        {
            this.consumer.Subscribe(KafkaConstants.PaymentFailedTopic);

            this.logger.LogInformation("PaymentRetry consumer started.");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    ConsumeResult<string, string> result = this.consumer.Consume(stoppingToken);
                    ProcessPaymentCommand? processPaymentCommand = JsonSerializer.Deserialize<ProcessPaymentCommand>(result.Message.Value);
                    if (processPaymentCommand == null)
                        continue;

                    this.logger.LogInformation("MESSAGE RECEIVED: {Message}", result.Message.Value);

                    int retryCount = result.Message.Headers.GetRetryCount();
                    if (retryCount >= MaxRetryCount)
                    {
                        await this.messageBus.PublishAsync(
                            topic: KafkaConstants.PaymentDeadLetterTopic,
                            message: processPaymentCommand,
                            headers: KafkaHeaderExtensions.CreateRetryHeaders(retryCount),
                            cancellationToken: stoppingToken);

                        this.logger.LogError(
                            """
                            MESSAGE SENT TO DEAD LETTER QUEUE
                            OrderId: {OrderId}
                            RetryCount: {RetryCount}
                            """,
                            processPaymentCommand.OrderId,
                            result.Message.Headers.GetRetryCount());

                        continue;
                    }

                    // Delay before retrying
                    await Task.Delay(RetryDelayMilliseconds, stoppingToken);

                    await this.messageBus.PublishAsync(
                       topic: KafkaConstants.ProcessPaymentTopic,
                       message: processPaymentCommand,
                       headers: KafkaHeaderExtensions.CreateRetryHeaders(retryCount),
                       cancellationToken: stoppingToken);

                    //this.logger.LogInformation("Payment retry published. OrderId: {OrderId}, NewRetryCount: {RetryCount}", processPaymentCommand.OrderId, retryCount);
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
