using System;
using Confluent.Kafka;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Kafka;

public class ConfigureProducerEventArgs : EventArgs
{
    private ProducerConfig _producerConfig;

    public ConfigureProducerEventArgs(ProducerConfig producerConfig)
    {
        _producerConfig = Guard.AgainstNull(producerConfig);
    }

    public ProducerConfig ProducerConfig
    {
        get => _producerConfig;
        set => _producerConfig = Guard.AgainstNull(value);
    }
}