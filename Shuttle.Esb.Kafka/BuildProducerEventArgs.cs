using System;
using Confluent.Kafka;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Kafka
{
    public class BuildProducerEventArgs : EventArgs
    {
        public ProducerBuilder<Null, string> ProducerBuilder { get; }

        public BuildProducerEventArgs(ProducerBuilder<Null, string> producerBuilder)
        {
            ProducerBuilder = Guard.AgainstNull(producerBuilder, nameof(producerBuilder));
        }
    }
}