﻿using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shuttle.Core.Pipelines;
using Shuttle.Esb.Logging;

namespace Shuttle.Esb.Kafka.Tests
{
    public class KafkaFixture
    {
        public static IServiceCollection GetServiceCollection(bool useCancellationToken = false, bool log = false)
        {
            var services = new ServiceCollection();

            services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());

            if (log)
            {
                services.AddServiceBusLogging(builder =>
                {
                    builder.Options.AddPipelineEventType<OnAbortPipeline>();
                    builder.Options.AddPipelineEventType<OnPipelineStarting>();
                    builder.Options.AddPipelineEventType<OnPipelineException>();
                    builder.Options.AddPipelineEventType<OnGetMessage>();
                });

                services.AddLogging(builder =>
                {
                    builder.SetMinimumLevel(LogLevel.Trace);
                    builder.AddConsole();
                });
            }

            services.AddKafka(builder =>
            {
                var kafkaOptions = new KafkaOptions
                {
                    BootstrapServers = "localhost:9092",
                    UseCancellationToken = useCancellationToken,
                    ConsumeTimeout = TimeSpan.FromSeconds(5),
                    ConnectionsMaxIdle = TimeSpan.FromSeconds(5)
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