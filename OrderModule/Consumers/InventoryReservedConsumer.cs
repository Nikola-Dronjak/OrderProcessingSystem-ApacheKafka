using Common.Commands;
using Common.Events;
using Common.Messaging;
using Common.Messaging.Kafka;
using Confluent.Kafka;
using System.Text.Json;

namespace OrderModule.Consumers
{
    public class InventoryReservedConsumer : KafkaConsumerBackgroundService
    {
        private const string GroupId = "inventory-reserved-group";
        private const int SimulatedOrchestrationProcessingDelayMilliseconds = 1000;

        private readonly IMessageBus messageBus;
        private readonly ILogger<InventoryReservedConsumer> logger;

        public InventoryReservedConsumer(IKafkaConsumerFactory consumerFactory, IMessageBus messageBus, ILogger<InventoryReservedConsumer> logger) : base(consumerFactory, GroupId)
        {
            this.messageBus = messageBus;
            this.logger = logger;
        }

        protected override async Task ConsumeMessagesAsync(CancellationToken stoppingToken)
        {
            this.consumer.Subscribe(KafkaConstants.InventoryReservedTopic);

            this.logger.LogInformation("InventoryReserved consumer started.");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    ConsumeResult<string, string> result = this.consumer.Consume(stoppingToken);
                    InventoryReservedEvent? inventoryReservedEvent = JsonSerializer.Deserialize<InventoryReservedEvent>(result.Message.Value);
                    if (inventoryReservedEvent == null)
                        continue;

                    this.logger.LogInformation("MESSAGE RECEIVED: {Message}", result.Message.Value);

                    // Simulate orchestration processing time
                    await Task.Delay(SimulatedOrchestrationProcessingDelayMilliseconds, stoppingToken);

                    ProcessPaymentCommand processPaymentCommand = new ProcessPaymentCommand
                    {
                        OrderId = inventoryReservedEvent.OrderId,
                        Price = inventoryReservedEvent.Price,
                        CorrelationId = inventoryReservedEvent.CorrelationId,
                        Timestamp = DateTime.UtcNow
                    };

                    await this.messageBus.PublishAsync(
                        topic: KafkaConstants.ProcessPaymentTopic,
                        message: processPaymentCommand,
                        cancellationToken: stoppingToken);

                    this.logger.LogInformation("Process payment for order {OrderId}", processPaymentCommand.OrderId);
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
