namespace Shuttle.Esb.Kafka
{
    public class KafkaOptions
    {
        public const string SectionName = "Shuttle:ServiceBus:Kafka";

        public string BootstrapServers { get; set; }
        public short ReplicationFactor { get; set; } = 1;
        public int NumPartitions { get; set; } = 1;
        public int MessageSendMaxRetries { get; set; } = 3;
        public int RetryBackoffMs { get; set; } = 1000;
    }
}