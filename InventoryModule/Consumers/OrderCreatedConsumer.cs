using Common.Events;
using Common.Messaging;
using Common.Messaging.Kafka;
using Confluent.Kafka;
using System.Text.Json;

namespace InventoryModule.Consumers
{
    public class OrderCreatedConsumer : KafkaConsumerBackgroundService
    {
        private const int SimulatedInventoryProcessingDelayMilliseconds = 500;
        private readonly IMessageBus messageBus;
        private readonly ILogger<OrderCreatedConsumer> logger;

        public OrderCreatedConsumer(IConsumer<string, string> consumer, IMessageBus messageBus, ILogger<OrderCreatedConsumer> logger) : base(consumer)
        {
            this.messageBus = messageBus;
            this.logger = logger;
        }

        protected override async Task ConsumeMessagesAsync(CancellationToken stoppingToken)
        {
            this.consumer.Subscribe(KafkaConstants.OrderCreatedTopic);

            this.logger.LogInformation("Inventory consumer started.");

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        ConsumeResult<string, string> result = this.consumer.Consume(stoppingToken);
                        OrderCreatedEvent? orderCreatedEvent = JsonSerializer.Deserialize<OrderCreatedEvent>(result.Message.Value);

                        if (orderCreatedEvent == null)
                            continue;

                        this.logger.LogInformation("MESSAGE RECEIVED: {Message}", result.Message.Value);

                        // Simulate inventory processing logic
                        await Task.Delay(SimulatedInventoryProcessingDelayMilliseconds, stoppingToken);

                        InventoryReservedEvent inventoryReservedEvent = new InventoryReservedEvent
                        {
                            OrderId = orderCreatedEvent.OrderId,
                            ProductId = orderCreatedEvent.ProductId,
                            Quantity = orderCreatedEvent.Quantity,
                            Price = orderCreatedEvent.Price,
                            CorrelationId = orderCreatedEvent.CorrelationId,
                            Timestamp = DateTime.UtcNow
                        };

                        await this.messageBus.PublishAsync(
                            topic: KafkaConstants.InventoryReservedTopic,
                            message: inventoryReservedEvent,
                            cancellationToken: stoppingToken);

                        this.logger.LogInformation("Inventory reserved for order {OrderId}", orderCreatedEvent.OrderId);
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
            finally
            {
                this.consumer.Close();
            }
        }
    }
}
