using Common.Messaging.Extensions;
using Common.Messaging.Kafka;
using Confluent.Kafka;
using InventoryModule.Consumers;
using Microsoft.Extensions.Options;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddKafkaMessaging(builder.Configuration);

builder.Services.AddSingleton<IConsumer<string, string>>(sp =>
{
    KafkaSettings settings = sp.GetRequiredService<IOptions<KafkaSettings>>().Value;
    ConsumerConfig config = new ConsumerConfig
    {
        BootstrapServers = settings.BootstrapServers,
        GroupId = "inventory-group",
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnableAutoCommit = true
    };

    return new ConsumerBuilder<string, string>(config).Build();
});

builder.Services.AddHostedService<OrderCreatedConsumer>();

var host = builder.Build();
host.Run();
