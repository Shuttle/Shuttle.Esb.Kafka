using System;
using Confluent.Kafka;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Kafka;

public class ConfigureConsumerEventArgs : EventArgs
{
    private ConsumerConfig _consumerConfig;

    public ConfigureConsumerEventArgs(ConsumerConfig consumerConfig)
    {
        _consumerConfig = Guard.AgainstNull(consumerConfig);
    }

    public ConsumerConfig ConsumerConfig
    {
        get => _consumerConfig;
        set => _consumerConfig = Guard.AgainstNull(value);
    }
}