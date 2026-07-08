using Confluent.Kafka;
using Microsoft.Extensions.Options;
using OrderModule.Configuration;
using OrderModule.Messaging;
using OrderModule.Services;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.Configure<KafkaSettings>(builder.Configuration.GetSection("Kafka"));
builder.Services.AddSingleton<IProducer<string, string>>(sp =>
{
    KafkaSettings settings = sp.GetRequiredService<IOptions<KafkaSettings>>().Value;

    ProducerConfig config = new ProducerConfig
    {
        BootstrapServers = settings.BootstrapServers
    };

    return new ProducerBuilder<string, string>(config).Build();
});
builder.Services.AddSingleton<IMessageBus, KafkaMessageBus>();
builder.Services.AddScoped<IOrderService, OrderService>();

builder.Services.AddControllers();

var app = builder.Build();

app.MapControllers();

app.Run();
