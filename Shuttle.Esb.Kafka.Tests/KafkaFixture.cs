using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shuttle.Esb.Logging;

namespace Shuttle.Esb.Kafka.Tests
{
    public class KafkaFixture
    {
        public static IServiceCollection GetServiceCollection(bool useCancellationToken = false)
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
                    BootstrapServers = "localhost:9092",
                    EnableAutoCommit = false,
                    EnableAutoOffsetStore = false,
                    FlushEnqueue = true,
                    UseCancellationToken = useCancellationToken,
                    ConsumeTimeout = TimeSpan.FromSeconds(5),
                    ConnectionsMaxIdle = TimeSpan.FromSeconds(5)
                });
            });

            return services;
        }
    }
}