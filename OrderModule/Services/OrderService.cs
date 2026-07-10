using Common.Events;
using Common.Messaging;
using Common.Messaging.Kafka;
using OrderModule.Models;

namespace OrderModule.Services
{
    public class OrderService : IOrderService
    {
        private readonly IMessageBus messageBus;
        private readonly ILogger<OrderService> logger;

        public OrderService(IMessageBus messageBus, ILogger<OrderService> logger)
        {
            this.messageBus = messageBus;
            this.logger = logger;
        }

        public async Task<Guid> CreateOrderAsync(CreateOrderRequest createOrderRequest, CancellationToken cancellationToken)
        {
            Guid orderId = Guid.NewGuid();
            string correlationId = Guid.NewGuid().ToString();
            OrderCreatedEvent orderCreatedEvent = new OrderCreatedEvent
            {
                OrderId = orderId,
                ProductId = createOrderRequest.ProductId,
                Quantity = createOrderRequest.Quantity,
                Price = createOrderRequest.Price,
                Timestamp = DateTime.UtcNow,
                CorrelationId = correlationId
            };

            logger.LogInformation("Publishing OrderCreatedEvent for OrderId: {OrderId}, CorrelationId: {CorrelationId}", orderId, correlationId);

            await messageBus.PublishAsync(
                topic: KafkaConstants.OrderCreatedTopic,
                message: orderCreatedEvent,
                cancellationToken: cancellationToken);

            return orderId;
        }
    }
}
