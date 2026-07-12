using Common.Commands;
using Common.Messaging.Kafka;
using Confluent.Kafka;
using System.Text.Json;

namespace PaymentModule.Consumers
{
    public class PaymentDeadLetterConsumer : KafkaConsumerBackgroundService
    {
        private const string GroupId = "payment-dead-letter-group";

        private readonly ILogger<PaymentDeadLetterConsumer> logger;

        public PaymentDeadLetterConsumer(IKafkaConsumerFactory consumerFactory, ILogger<PaymentDeadLetterConsumer> logger) : base(consumerFactory, GroupId)
        {
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
