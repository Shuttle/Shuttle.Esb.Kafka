using System;
using Confluent.Kafka;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Kafka
{
    public class ConfigureProducerEventArgs : EventArgs
    {
        private ProducerConfig _producerConfig;

        public ProducerConfig ProducerConfig
        {
            get => _producerConfig;
            set => _producerConfig = value ?? throw new System.ArgumentNullException();
        }

        public ConfigureProducerEventArgs(ProducerConfig producerConfig)
        {
            Guard.AgainstNull(producerConfig, nameof(producerConfig));

            _producerConfig = producerConfig;
        }
    }
}