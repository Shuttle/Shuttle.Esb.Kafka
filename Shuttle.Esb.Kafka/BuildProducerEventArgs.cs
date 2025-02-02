using System;
using Confluent.Kafka;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Kafka;

public class BuildProducerEventArgs : EventArgs
{
    public BuildProducerEventArgs(ProducerBuilder<Null, string> producerBuilder)
    {
        ProducerBuilder = Guard.AgainstNull(producerBuilder);
    }

    public ProducerBuilder<Null, string> ProducerBuilder { get; }
}