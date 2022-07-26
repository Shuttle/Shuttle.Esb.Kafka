using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Reflection;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using Shuttle.Core.Streams;

namespace Shuttle.Esb.Kafka
{
    public class KafkaQueue : IQueue, ICreateQueue, IDropQueue, IPurgeQueue, IDisposable
    {
        private readonly IConsumer<Ignore, string> _consumer;
        private readonly KafkaOptions _kafkaOptions;

        private readonly object _lock = new object();

        private readonly TimeSpan _operationTimeout = TimeSpan.FromSeconds(30);
        private readonly IProducer<Null, string> _producer;
        private readonly Queue<ReceivedMessage> _receivedMessages = new Queue<ReceivedMessage>();
        private bool _subscribed;

        public KafkaQueue(Uri uri, IOptionsMonitor<KafkaOptions> kafkaOptions)
        {
            Guard.AgainstNull(kafkaOptions, nameof(kafkaOptions));

            Uri = uri;

            var parser = new KafkaQueueUriParser(uri);

            _kafkaOptions = kafkaOptions.Get(parser.ConfigurationName);

            if (_kafkaOptions == null)
            {
                throw new InvalidOperationException(string.Format(Resources.ConfigurationNameMissing,
                    parser.ConfigurationName));
            }

            Topic = parser.Topic;

            var entryAssembly = Assembly.GetEntryAssembly();

            if (entryAssembly == null)
            {
                throw new ArgumentNullException(nameof(entryAssembly));
            }

            _consumer = new ConsumerBuilder<Ignore, string>(new ConsumerConfig
            {
                BootstrapServers = _kafkaOptions.BootstrapServers,
                GroupId = Topic,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false,
                ConnectionsMaxIdleMs = 0,
                ReconnectBackoffMaxMs = 5,
                ReconnectBackoffMs = 5,

                //EnableAutoCommit = false,
                //EnableAutoOffsetStore = false
            }).Build();

            _producer = new ProducerBuilder<Null, string>(new ProducerConfig
            {
                BootstrapServers = _kafkaOptions.BootstrapServers,
                ClientId = Dns.GetHostName(),
                Acks = Acks.All,
                MessageSendMaxRetries = _kafkaOptions.MessageSendMaxRetries,
                RetryBackoffMs = _kafkaOptions.RetryBackoffMs,
                EnableIdempotence = true
            }).Build();
        }

        public string Topic { get; }

        public void Create()
        {
            lock (_lock)
            {
                using (var client = new AdminClientBuilder(new AdminClientConfig
                       {
                           BootstrapServers = _kafkaOptions.BootstrapServers
                       }).Build())
                {
                    var metadata = client.GetMetadata(Topic, _operationTimeout);

                    if (metadata == null)
                    {
                        client.CreateTopicsAsync(new[]
                        {
                            new TopicSpecification
                            {
                                Name = Topic,
                                ReplicationFactor = _kafkaOptions.ReplicationFactor,
                                NumPartitions = _kafkaOptions.NumPartitions
                            }
                        }).Wait(_operationTimeout);
                    }
                }
            }
        }

        public void Dispose()
        {
            try
            {
                _producer?.Flush(_operationTimeout);
            }
            catch
            {
                // ignore
            }

            try
            {
                _consumer.Unsubscribe();
                _consumer.Close();
            }
            catch
            {
                // ignore
            }

            _producer?.Dispose();
            _consumer?.Dispose();
        }

        public void Drop()
        {
            lock (_lock)
            {
                using (var client = new AdminClientBuilder(new AdminClientConfig
                       {
                           BootstrapServers = _kafkaOptions.BootstrapServers
                       }).Build())
                {
                    var metadata = client.GetMetadata(Topic, _operationTimeout);

                    if (metadata == null)
                    {
                        return;
                    }

                    try
                    {
                        client.DeleteTopicsAsync(new List<string>
                        {
                            Topic
                        }, new DeleteTopicsOptions { OperationTimeout = _operationTimeout }).Wait(_operationTimeout);
                    }
                    catch (AggregateException ex) when (ex.InnerException is DeleteTopicsException)
                    {
                    }
                }
            }
        }

        public void Purge()
        {
            Drop();
            Create();
        }

        public bool IsEmpty()
        {
            lock (_lock)
            {
                if (_receivedMessages.Count > 0)
                {
                    return false;
                }

                ReadMessage();

                return _receivedMessages.Count == 0;
            }
        }

        public void Enqueue(TransportMessage message, Stream stream)
        {
            Guard.AgainstNull(message, nameof(message));
            Guard.AgainstNull(stream, nameof(stream));

            lock (_lock)
            {
                _producer.Produce(Topic,
                    new Message<Null, string> { Value = Convert.ToBase64String(stream.ToBytes()) });
            }
        }

        public ReceivedMessage GetMessage()
        {
            lock (_lock)
            {
                if (_receivedMessages.Count > 0)
                {
                    return _receivedMessages.Dequeue();
                }

                ReadMessage();

                return _receivedMessages.Count > 0 ? _receivedMessages.Dequeue() : null;
            }
        }

        private void ReadMessage()
        {
            if (!_subscribed)
            {
                try
                {
                    _consumer.Subscribe(Topic);
                    _subscribed = true;
                }
                catch (Exception)
                {
                    return;
                }
            }

            var consumeResult = _consumer.Consume(1000);

            if (consumeResult == null)
            {
                return;
            }

            var acknowledgementToken = new AcknowledgementToken(Guid.NewGuid(), consumeResult);

            _receivedMessages.Enqueue(new ReceivedMessage(new MemoryStream(Convert.FromBase64String(consumeResult.Message.Value)), acknowledgementToken));
        }

        public void Acknowledge(object acknowledgementToken)
        {
            Guard.AgainstNull(acknowledgementToken, nameof(acknowledgementToken));

            lock (_lock)
            {
                var token = (AcknowledgementToken)acknowledgementToken;

                _consumer.StoreOffset(token.ConsumeResult);
                _consumer.Commit(token.ConsumeResult);
            }
        }

        public void Release(object acknowledgementToken)
        {
            lock (_lock)
            {
                var token = (AcknowledgementToken)acknowledgementToken;

                _receivedMessages.Enqueue(new ReceivedMessage(
                    new MemoryStream(Convert.FromBase64String(token.ConsumeResult.Message.Value)),
                    acknowledgementToken));
            }
        }

        public Uri Uri { get; }
        public bool IsStream => true;

        internal class AcknowledgementToken
        {
            public AcknowledgementToken(Guid messageId, ConsumeResult<Ignore, string> consumeResult)
            {
                MessageId = messageId;
                ConsumeResult = consumeResult;
            }

            public Guid MessageId { get; }
            public ConsumeResult<Ignore, string> ConsumeResult { get; }
        }
    }
}