using System;
using Confluent.Kafka;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Kafka
{
    public class ConfigureConsumerEventArgs : EventArgs
    {
        private ConsumerConfig _consumerConfig;

        public ConsumerConfig ConsumerConfig
        {
            get => _consumerConfig;
            set => _consumerConfig = value ?? throw new System.ArgumentNullException();
        }

        public ConfigureConsumerEventArgs(ConsumerConfig consumerConfig)
        {
            Guard.AgainstNull(consumerConfig, nameof(consumerConfig));

            _consumerConfig = consumerConfig;
        }
    }
}