using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;
using Shuttle.Core.Contract;

namespace Shuttle.Esb.Kafka
{
    public class KafkaBuilder
    {
        public IServiceCollection Services { get; }

        internal readonly Dictionary<string, KafkaOptions> KafkaOptions = new Dictionary<string, KafkaOptions>();

        public KafkaBuilder(IServiceCollection services)
        {
            Guard.AgainstNull(services, nameof(services));

            Services = services;
        }

        public KafkaBuilder AddOptions(string name, KafkaOptions kafkaOptions)
        {
            Guard.AgainstNullOrEmptyString(name, nameof(name));
            Guard.AgainstNull(kafkaOptions, nameof(kafkaOptions));

            KafkaOptions.Remove(name);

            KafkaOptions.Add(name, kafkaOptions);

            return this;
        }
    }
}