using Common.Messaging.Kafka;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Common.Messaging.Extensions
{
    public static class ServiceCollectionExtension
    {
        public static IServiceCollection AddKafkaMessaging(this IServiceCollection services, IConfiguration configuration)
        {
            services.Configure<KafkaSettings>(configuration.GetSection("Kafka"));
            services.AddSingleton<IProducer<string, string>>(sp =>
            {
                KafkaSettings settings = sp.GetRequiredService<IOptions<KafkaSettings>>().Value;
                ProducerConfig producerConfig = new ProducerConfig
                {
                    BootstrapServers = settings.BootstrapServers,
                    Acks = Acks.All,
                    EnableIdempotence = true
                };

                return new ProducerBuilder<string, string>(producerConfig).Build();
            });
            services.AddSingleton<IMessageBus, KafkaMessageBus>();
            services.AddSingleton<IKafkaConsumerFactory, KafkaConsumerFactory>();
            return services;
        }
    }
}
