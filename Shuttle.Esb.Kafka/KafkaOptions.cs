using System;
using Confluent.Kafka;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Kafka;

public class KafkaOptions
{
    public const string SectionName = "Shuttle:Kafka";
    public Acks Acks { get; set; } = Acks.All;

    public string BootstrapServers { get; set; } = string.Empty;
    public TimeSpan ConnectionsMaxIdle { get; set; } = TimeSpan.Zero;
    public TimeSpan ConsumeTimeout { get; set; } = TimeSpan.FromSeconds(30);
    public bool EnableAutoCommit { get; set; }
    public bool EnableAutoOffsetStore { get; set; }
    public bool EnableIdempotence { get; set; } = true;
    public bool FlushEnqueue { get; set; }
    public int MessageSendMaxRetries { get; set; } = 3;
    public int NumPartitions { get; set; } = 1;
    public TimeSpan OperationTimeout { get; set; } = TimeSpan.FromSeconds(30);
    public short ReplicationFactor { get; set; } = 1;
    public TimeSpan RetryBackoff { get; set; } = TimeSpan.FromSeconds(1);
    public bool UseCancellationToken { get; set; } = true;

    public event EventHandler<BuildConsumerEventArgs>? BuildConsumer;
    public event EventHandler<BuildProducerEventArgs>? BuildProducer;
    public event EventHandler<ConfigureConsumerEventArgs>? ConfigureConsumer;
    public event EventHandler<ConfigureProducerEventArgs>? ConfigureProducer;

    public void OnBuildConsumer(object? sender, BuildConsumerEventArgs args)
    {
        BuildConsumer?.Invoke(Guard.AgainstNull(sender), Guard.AgainstNull(args));
    }

    public void OnBuildProducer(object? sender, BuildProducerEventArgs args)
    {
        BuildProducer?.Invoke(Guard.AgainstNull(sender), Guard.AgainstNull(args));
    }

    public void OnConfigureConsumer(object? sender, ConfigureConsumerEventArgs args)
    {
        ConfigureConsumer?.Invoke(Guard.AgainstNull(sender), Guard.AgainstNull(args));
    }

    public void OnConfigureProducer(object? sender, ConfigureProducerEventArgs args)
    {
        ConfigureProducer?.Invoke(Guard.AgainstNull(sender), Guard.AgainstNull(args));
    }
}