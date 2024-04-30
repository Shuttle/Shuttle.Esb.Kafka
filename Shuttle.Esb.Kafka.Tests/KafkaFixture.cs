using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Shuttle.Esb.Kafka.Tests
{
    public class KafkaConfiguration
    {
        public static IServiceCollection GetServiceCollection(bool useCancellationToken = false)
        {
            var services = new ServiceCollection();

            services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());

            services.AddKafka(builder =>
            {
                var kafkaOptions = new KafkaOptions
                {
                    BootstrapServers = "localhost:9092",
                    UseCancellationToken = useCancellationToken,
                    ConsumeTimeout = TimeSpan.FromSeconds(5),
                    ConnectionsMaxIdle = TimeSpan.FromSeconds(5),
                };

                kafkaOptions.ConfigureConsumer += (sender, args) =>
                {
                    Console.WriteLine($"[event] : ConfigureConsumer / Uri = '{((IQueue)sender).Uri}'");
                };

                kafkaOptions.ConfigureProducer += (sender, args) =>
                {
                    Console.WriteLine($"[event] : ConfigureProducer / Uri = '{((IQueue)sender).Uri}'");
                };

                builder.AddOptions("local", kafkaOptions);
            });

            return services;
        }
    }
}