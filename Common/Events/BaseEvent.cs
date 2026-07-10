namespace Common.Events
{
    public class BaseEvent
    {
        public string CorrelationId { get; set; }
        public DateTime Timestamp { get; set; }
    }
}
