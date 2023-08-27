using NUnit.Framework;
using Shuttle.Esb.Tests;

namespace Shuttle.Esb.Kafka.Tests
{
    public class KafkaInboxFixture : InboxFixture
    {
        [TestCase(true, true)]
        [TestCase(true, false)]
        [TestCase(false, true)]
        [TestCase(false, false)]
        public async Task Should_be_able_handle_errors(bool hasErrorQueue, bool isTransactionalEndpoint)
        {
            await TestInboxError(KafkaFixture.GetServiceCollection(), "kafka://local/{0}", hasErrorQueue, isTransactionalEndpoint);
        }

        [TestCase(250, false)]
        [TestCase(250, true)]
        public async Task Should_be_able_to_process_messages_concurrently(int msToComplete, bool isTransactionalEndpoint)
        {
            await TestInboxConcurrency(KafkaFixture.GetServiceCollection(), "kafka://local/{0}", msToComplete, isTransactionalEndpoint);
        }

        [TestCase(100, true)]
        [TestCase(100, false)]
        public async Task Should_be_able_to_process_queue_timeously(int count, bool isTransactionalEndpoint)
        {
            await TestInboxThroughput(KafkaFixture.GetServiceCollection(true), "kafka://local/{0}", 1000, count, 1, isTransactionalEndpoint);
        }
    }
}