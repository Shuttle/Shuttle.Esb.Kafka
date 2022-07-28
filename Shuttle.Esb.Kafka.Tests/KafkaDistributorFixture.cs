using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.Kafka.Tests
{
    public class KafkaDistributorFixture : DistributorFixture
    {
        [Test]
        [TestCase(false)]
        [TestCase(true)]
        public void Should_be_able_to_distribute_messages(bool isTransactionalEndpoint)
        {
            TestDistributor(KafkaFixture.GetServiceCollection(), 
                KafkaFixture.GetServiceCollection(), @"kafka://local/{0}", isTransactionalEndpoint, 30);
        }
    }
}