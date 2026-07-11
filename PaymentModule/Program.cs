using Common.Messaging.Extensions;
using Common.Messaging.Kafka;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using PaymentModule.Consumers;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddKafkaMessaging(builder.Configuration);

builder.Services.AddSingleton<IConsumer<string, string>>(sp =>
{
    KafkaSettings settings = sp.GetRequiredService<IOptions<KafkaSettings>>().Value;
    ConsumerConfig config = new ConsumerConfig
    {
        BootstrapServers = settings.BootstrapServers,
        GroupId = "payment-group",
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnableAutoCommit = true
    };

    return new ConsumerBuilder<string, string>(config).Build();
});

builder.Services.AddHostedService<ProcessPaymentConsumer>();

var host = builder.Build();
host.Run();
