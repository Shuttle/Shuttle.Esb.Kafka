using System;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using Shuttle.Core.Threading;

namespace Shuttle.Esb.Kafka
{
    public class KafkaQueueFactory : IQueueFactory
    {
        private readonly IOptionsMonitor<KafkaOptions> _kafkaOptions;
        private readonly ICancellationTokenSource _cancellationTokenSource;

        public KafkaQueueFactory(IOptionsMonitor<KafkaOptions> kafkaOptions, ICancellationTokenSource cancellationTokenSource)
        {
            Guard.AgainstNull(kafkaOptions, nameof(kafkaOptions));
            Guard.AgainstNull(cancellationTokenSource, nameof(cancellationTokenSource));

            _kafkaOptions = kafkaOptions;
            _cancellationTokenSource = cancellationTokenSource;
        }

        public IQueue Create(Uri uri)
        {
            Guard.AgainstNull(uri, "uri");

            return new KafkaQueue(uri, _kafkaOptions, _cancellationTokenSource.Get().Token);
        }

        public string Scheme => KafkaQueue.Scheme;
    }
}
