namespace Common.Messaging.Kafka
{
    public class KafkaConstants
    {
        #region Topic names
        public const string OrderCreatedTopic = "order-created";
        public const string InventoryReservedTopic = "inventory-reserved";
        public const string ProcessPaymentTopic = "process-payment";
        public const string PaymentSucceededTopic = "payment-succeeded";
        public const string PaymentFailedTopic = "payment-failed";
        public const string PaymentDeadLetterTopic = "payment-dead-letter";
        public const string OrderCompletedTopic = "order-completed";
        #endregion

        #region Header names
        public const string RetryCountHeader = "retry-count";
        #endregion
    }
}
