using Common.Messaging.Kafka;
using Confluent.Kafka;
using System.Text;

namespace Common.Messaging.Extensions
{
    public static class KafkaHeaderExtensions
    {
        public static int GetRetryCount(this Headers headers)
        {
            if (headers.TryGetLastBytes(KafkaConstants.RetryCountHeader, out var retryCountBytes))
            {
                string retryCountString = Encoding.UTF8.GetString(retryCountBytes);
                if (int.TryParse(retryCountString, out var retryCount))
                {
                    return retryCount;
                }
            }
            return 0;
        }

        public static Headers CreateRetryHeaders (int retryCount)
        {
            Headers headers = new Headers
            {
                { KafkaConstants.RetryCountHeader, Encoding.UTF8.GetBytes(retryCount.ToString()) }
            };
            return headers; 
        }
    }
}
