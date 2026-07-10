using Common.Messaging.Extensions;
using OrderModule.Services;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddKafkaMessaging(builder.Configuration);

builder.Services.AddScoped<IOrderService, OrderService>();

builder.Services.AddControllers();

var app = builder.Build();

app.MapControllers();

app.Run();
