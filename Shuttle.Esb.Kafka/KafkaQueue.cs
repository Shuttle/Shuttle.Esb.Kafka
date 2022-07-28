using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Reflection;
using System.Threading;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Options;
using Shuttle.Core.Contract;
using Shuttle.Core.Streams;

namespace Shuttle.Esb.Kafka
{
    public class KafkaQueue : IQueue, ICreateQueue, IDropQueue, IPurgeQueue, IDisposable
    {
        private readonly CancellationToken _cancellationToken;
        private IConsumer<Ignore, string> _consumer;
        private readonly KafkaOptions _kafkaOptions;

        private readonly object _lock = new object();

        private readonly TimeSpan _operationTimeout;
        private IProducer<Null, string> _producer;
        private readonly Queue<ReceivedMessage> _receivedMessages = new Queue<ReceivedMessage>();
        private bool _subscribed;
        private bool _disposed;
        private readonly ConsumerConfig _consumerConfig;

        public KafkaQueue(Uri uri, IOptionsMonitor<KafkaOptions> kafkaOptions, CancellationToken cancellationToken)
        {
            Guard.AgainstNull(kafkaOptions, nameof(kafkaOptions));
            Guard.AgainstNull(cancellationToken, nameof(cancellationToken));

            Uri = uri;

            var parser = new KafkaQueueUriParser(uri);

            _cancellationToken = cancellationToken;
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

            _operationTimeout = _kafkaOptions.OperationTimeout;

            _consumerConfig = new ConsumerConfig
            {
                BootstrapServers = _kafkaOptions.BootstrapServers,
                GroupId = Topic,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = _kafkaOptions.EnableAutoCommit,
                EnableAutoOffsetStore = _kafkaOptions.EnableAutoOffsetStore,
                ConnectionsMaxIdleMs = (int)_kafkaOptions.ConnectionsMaxIdle.TotalMilliseconds
            };

            _kafkaOptions.OnConfigureConsumer(this, new ConfigureConsumerEventArgs(_consumerConfig));

            _consumer = new ConsumerBuilder<Ignore, string>(_consumerConfig).Build();

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = _kafkaOptions.BootstrapServers,
                ClientId = Dns.GetHostName(),
                Acks = Acks.All,
                MessageSendMaxRetries = _kafkaOptions.MessageSendMaxRetries,
                RetryBackoffMs = (int)_kafkaOptions.RetryBackoff.TotalMilliseconds,
                EnableIdempotence = true,
                ConnectionsMaxIdleMs = (int)_kafkaOptions.ConnectionsMaxIdle.TotalMilliseconds
            };

            _kafkaOptions.OnConfigureProducer(this, new ConfigureProducerEventArgs(producerConfig));

            _producer = new ProducerBuilder<Null, string>(producerConfig).Build();
        }

        public string Topic { get; }

        public void Create()
        {
            lock (_lock)
            {
                using (var client = new AdminClientBuilder(new AdminClientConfig
                       {
                           BootstrapServers = _consumerConfig.BootstrapServers
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
            lock (_lock)
            {
                if (_disposed)
                {
                    return;
                }

                try
                {
                    _producer?.Flush(_operationTimeout);
                }
                catch
                {
                    // ignore
                }

                _producer?.Dispose();
                _producer = null;

                try
                {
                    _consumer?.Unsubscribe();
                    _consumer?.Close();
                }
                catch
                {
                    // ignore
                }

                _consumer?.Dispose();
                _consumer = null;
                
                _disposed = true;
            }
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
                if (_receivedMessages.Count > 0 || _disposed)
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
                if (_disposed)
                {
                    return;
                }

                _producer.Produce(Topic,
                    new Message<Null, string> { Value = Convert.ToBase64String(stream.ToBytes()) });

                if (!_kafkaOptions.FlushEnqueue)
                {
                    return;
                }

                if (_kafkaOptions.UseCancellationToken)
                {
                    try
                    {
                        _producer.Flush(_cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                    }
                }
                else
                {
                    _producer.Flush(_operationTimeout);
                }
            }
        }

        public ReceivedMessage GetMessage()
        {
            lock (_lock)
            {
                if (_disposed)
                {
                    return null;
                }

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

            ConsumeResult<Ignore, string> consumeResult = null;


            try
            {
                consumeResult = _kafkaOptions.UseCancellationToken ? 
                    _consumer.Consume(_cancellationToken) : 
                    _consumer.Consume(_kafkaOptions.ConsumeTimeout);
            }
            catch (OperationCanceledException)
            {
            }

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
                if (_disposed)
                {
                    return;
                }

                if (!(_consumerConfig.EnableAutoCommit ?? false) && 
                    !(_consumerConfig.EnableAutoOffsetStore ?? false))
                {
                    var token = (AcknowledgementToken)acknowledgementToken;

                    if (!(_consumerConfig.EnableAutoCommit ?? false))
                    {
                        _consumer.Commit(token.ConsumeResult);
                    }

                    if (!(_consumerConfig.EnableAutoOffsetStore ?? false))
                    {
                        _consumer.StoreOffset(token.ConsumeResult);
                    }
                }
            }
        }

        public void Release(object acknowledgementToken)
        {
            lock (_lock)
            {
                if (_disposed)
                {
                    return;
                }

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