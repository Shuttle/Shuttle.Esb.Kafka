using System;
using Confluent.Kafka;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Kafka
{
    public class KafkaOptions
    {
        public const string SectionName = "Shuttle:ServiceBus:Kafka";

        public string BootstrapServers { get; set; }
        public short ReplicationFactor { get; set; } = 1;
        public int NumPartitions { get; set; } = 1;
        public int MessageSendMaxRetries { get; set; } = 3;
        public TimeSpan RetryBackoff { get; set; } = TimeSpan.FromSeconds(1);
        public bool EnableAutoCommit { get; set; }
        public bool EnableAutoOffsetStore { get; set; }
        public bool FlushEnqueue { get; set; }
        public bool UseCancellationToken { get; set; } = true;
        public TimeSpan ConsumeTimeout { get; set; } = TimeSpan.FromSeconds(30);
        public TimeSpan OperationTimeout { get; set; } = TimeSpan.FromSeconds(30);
        public TimeSpan ConnectionsMaxIdle { get; set; }
        public Acks Acks { get; set; } = Acks.All;
        public bool EnableIdempotence { get; set; } = true;

        public event EventHandler<ConfigureConsumerEventArgs> ConfigureConsumer = delegate
        {
        };

        public event EventHandler<ConfigureProducerEventArgs> ConfigureProducer = delegate
        {
        };

        public void OnConfigureConsumer(object sender, ConfigureConsumerEventArgs args)
        {
            Guard.AgainstNull(sender, nameof(sender));
            Guard.AgainstNull(args, nameof(args));

            ConfigureConsumer.Invoke(sender, args);
        }

        public void OnConfigureProducer(object sender, ConfigureProducerEventArgs args)
        {
            Guard.AgainstNull(sender, nameof(sender));
            Guard.AgainstNull(args, nameof(args));

            ConfigureProducer.Invoke(sender, args);
        }
    }
}