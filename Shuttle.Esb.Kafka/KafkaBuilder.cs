using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Kafka;

public class KafkaBuilder
{
    internal readonly Dictionary<string, KafkaOptions> KafkaOptions = new();

    public KafkaBuilder(IServiceCollection services)
    {
        Services = Guard.AgainstNull(services);
    }

    public IServiceCollection Services { get; }

    public KafkaBuilder AddOptions(string name, KafkaOptions kafkaOptions)
    {
        Guard.AgainstNullOrEmptyString(name);
        Guard.AgainstNull(kafkaOptions);

        KafkaOptions.Remove(name);

        KafkaOptions.Add(name, kafkaOptions);

        return this;
    }
}