using System;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Kafka
{
    public class KafkaQueueFactory : IQueueFactory
    {
        private readonly IOptionsMonitor<KafkaOptions> _kafkaOptions;

        public KafkaQueueFactory(IOptionsMonitor<KafkaOptions> kafkaOptions)
        {
            Guard.AgainstNull(kafkaOptions, nameof(kafkaOptions));

            _kafkaOptions = kafkaOptions;
        }

        public IQueue Create(Uri uri)
        {
            Guard.AgainstNull(uri, "uri");

            return new KafkaQueue(uri, _kafkaOptions);
        }

        public string Scheme => KafkaQueueUriParser.Scheme;
    }
}
