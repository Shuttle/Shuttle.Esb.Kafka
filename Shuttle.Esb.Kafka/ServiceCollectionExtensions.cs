using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Kafka
{
    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddKafka(this IServiceCollection services,
            Action<KafkaBuilder> builder = null)
        {
            Guard.AgainstNull(services, nameof(services));

            var kafkaBuilder = new KafkaBuilder(services);

            builder?.Invoke(kafkaBuilder);

            foreach (var pair in kafkaBuilder.KafkaOptions)
            {
                services.AddOptions<KafkaOptions>(pair.Key).Configure(options =>
                {
                    options.BootstrapServers = pair.Value.BootstrapServers;
                    options.MessageSendMaxRetries = pair.Value.MessageSendMaxRetries;
                    options.NumPartitions = pair.Value.NumPartitions;
                    options.ReplicationFactor = pair.Value.ReplicationFactor;
                    options.RetryBackoffMs = pair.Value.RetryBackoffMs;
                });
            }

            services.TryAddSingleton<IQueueFactory, KafkaQueueFactory>();

            return services;
        }
    }
}