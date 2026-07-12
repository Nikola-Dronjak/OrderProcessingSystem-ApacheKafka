using Common.Messaging.Extensions;
using PaymentModule.Consumers;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddKafkaMessaging(builder.Configuration);

builder.Services.AddHostedService<ProcessPaymentConsumer>();
builder.Services.AddHostedService<PaymentRetryConsumer>();
builder.Services.AddHostedService<PaymentDeadLetterConsumer>();

var host = builder.Build();
host.Run();
