using Common.Messaging.Extensions;
using OrderModule.Consumers;
using OrderModule.Services;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddKafkaMessaging(builder.Configuration);

builder.Services.AddScoped<IOrderService, OrderService>();

builder.Services.AddHostedService<InventoryReservedConsumer>();
builder.Services.AddHostedService<PaymentSuceededConsumer>();

builder.Services.AddControllers();
builder.Services.AddHealthChecks();

var app = builder.Build();

app.MapControllers();
app.MapHealthChecks("/health");

app.Run();
