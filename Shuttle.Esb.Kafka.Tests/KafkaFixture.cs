using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shuttle.Esb.Logging;

namespace Shuttle.Esb.Kafka.Tests
{
    public class KafkaFixture
    {
        public static IServiceCollection GetServiceCollection()
        {
            var services = new ServiceCollection();

            services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());

            services.AddServiceBusLogging(builder =>
            {
                builder.Options.AddPipelineEventType<OnGetMessage>();
            });

            services.AddLogging(builder =>
            {
                builder.SetMinimumLevel(LogLevel.Trace);
                builder.AddConsole();
            });

            services.AddKafka(builder =>
            {
                builder.AddOptions("local", new KafkaOptions
                {
                    BootstrapServers = "localhost:9092"
                });
            });

            return services;
        }
    }
}