namespace Common.Messaging.Kafka
{
    public class KafkaConstants
    {
        #region Topic names
        public const string OrderCreatedTopic = "order-created";
        public const string InventoryReservedTopic = "inventory-reserved";
        public const string ProcessPaymentTopic = "process-payment";
        public const string PaymentSucceededTopic = "payment-succeeded";
        #endregion
    }
}
