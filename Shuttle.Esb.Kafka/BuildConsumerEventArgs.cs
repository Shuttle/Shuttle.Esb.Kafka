using System;
using Confluent.Kafka;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Kafka
{
    public class BuildConsumerEventArgs : EventArgs
    {
        public ConsumerBuilder<Ignore, string> ConsumerBuilder { get; }

        public BuildConsumerEventArgs(ConsumerBuilder<Ignore, string> consumerBuilder)
        {
            ConsumerBuilder = Guard.AgainstNull(consumerBuilder, nameof(consumerBuilder));
        }
    }
}