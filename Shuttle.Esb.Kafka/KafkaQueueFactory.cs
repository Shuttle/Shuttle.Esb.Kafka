using System;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using Shuttle.Core.Threading;

namespace Shuttle.Esb.Kafka;

public class KafkaQueueFactory : IQueueFactory
{
    private readonly ICancellationTokenSource _cancellationTokenSource;
    private readonly IOptionsMonitor<KafkaOptions> _kafkaOptions;

    public KafkaQueueFactory(IOptionsMonitor<KafkaOptions> kafkaOptions, ICancellationTokenSource cancellationTokenSource)
    {
        _kafkaOptions = Guard.AgainstNull(kafkaOptions);
        _cancellationTokenSource = Guard.AgainstNull(cancellationTokenSource);
    }

    public IQueue Create(Uri uri)
    {
        var queueUri = new QueueUri(Guard.AgainstNull(uri)).SchemeInvariant(Scheme);
        var kafkaOptions = _kafkaOptions.Get(queueUri.ConfigurationName);

        if (kafkaOptions == null)
        {
            throw new InvalidOperationException(string.Format(Resources.QueueConfigurationNameException, queueUri.ConfigurationName));
        }

        return new KafkaQueue(queueUri, kafkaOptions, _cancellationTokenSource.Get().Token);
    }

    public string Scheme => "kafka";
}