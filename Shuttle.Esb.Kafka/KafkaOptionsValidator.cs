﻿using Microsoft.Extensions.Options;

namespace Shuttle.Esb.Kafka;

public class KafkaOptionsValidator : IValidateOptions<KafkaOptions>
{
    public ValidateOptionsResult Validate(string? name, KafkaOptions options)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return ValidateOptionsResult.Fail(Resources.QueueConfigurationNameException);
        }

        if (string.IsNullOrWhiteSpace(options.BootstrapServers))
        {
            return ValidateOptionsResult.Fail(string.Format(Resources.QueueConfigurationItemException, name, nameof(options.BootstrapServers)));
        }

        return ValidateOptionsResult.Success;
    }
}