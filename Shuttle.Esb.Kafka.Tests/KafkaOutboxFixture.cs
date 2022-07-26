using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.Kafka.Tests
{
    public class KafkaOutboxFixture : OutboxFixture
    {
        [TestCase(true)]
        [TestCase(false)]
        public void Should_be_able_to_use_outbox(bool isTransactionalEndpoint)
        {
            TestOutboxSending(KafkaFixture.GetServiceCollection(), "kafka://local/{0}", 1, isTransactionalEndpoint);
        }
    }
}