﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Shuttle.Core.Contract;
using Shuttle.Core.Streams;
using Exception = System.Exception;

namespace Shuttle.Esb.Kafka;

public class KafkaQueue : IQueue, ICreateQueue, IDropQueue, IPurgeQueue, IDisposable
{
    private readonly CancellationToken _cancellationToken;
    private readonly ConsumerConfig _consumerConfig;
    private readonly KafkaOptions _kafkaOptions;

    private readonly SemaphoreSlim _lock = new(1, 1);

    private readonly TimeSpan _operationTimeout;
    private readonly Queue<ReceivedMessage> _receivedMessages = new();
    private readonly IConsumer<Ignore, string> _consumer;
    private readonly IProducer<Null, string> _producer;
    private bool _subscribed;

    private bool _disposed;

    public KafkaQueue(QueueUri uri, KafkaOptions kafkaOptions, CancellationToken cancellationToken)
    {
        Uri = Guard.AgainstNull(uri);
        Topic = Uri.QueueName;

        _cancellationToken = Guard.AgainstNull(cancellationToken);
        _kafkaOptions = Guard.AgainstNull(kafkaOptions);
        _operationTimeout = _kafkaOptions.OperationTimeout;

        _consumerConfig = new()
        {
            BootstrapServers = _kafkaOptions.BootstrapServers,
            GroupId = Topic,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = _kafkaOptions.EnableAutoCommit,
            EnableAutoOffsetStore = _kafkaOptions.EnableAutoOffsetStore,
            ConnectionsMaxIdleMs = (int)_kafkaOptions.ConnectionsMaxIdle.TotalMilliseconds
        };

        _kafkaOptions.OnConfigureConsumer(this, new(_consumerConfig));

        var consumerBuilder = new ConsumerBuilder<Ignore, string>(_consumerConfig);

        _kafkaOptions.OnBuildConsumer(this, new(consumerBuilder));

        _consumer = consumerBuilder.Build();

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers,
            ClientId = Dns.GetHostName(),
            Acks = _kafkaOptions.Acks,
            MessageSendMaxRetries = _kafkaOptions.MessageSendMaxRetries,
            RetryBackoffMs = (int)_kafkaOptions.RetryBackoff.TotalMilliseconds,
            EnableIdempotence = _kafkaOptions.EnableIdempotence,
            ConnectionsMaxIdleMs = (int)_kafkaOptions.ConnectionsMaxIdle.TotalMilliseconds
        };

        _kafkaOptions.OnConfigureProducer(this, new(producerConfig));

        var producerBuilder = new ProducerBuilder<Null, string>(producerConfig);

        _kafkaOptions.OnBuildProducer(this, new(producerBuilder));

        _producer = producerBuilder.Build();
    }

    public string Topic { get; }

    public async Task CreateAsync()
    {
        if (_cancellationToken.IsCancellationRequested)
        {
            Operation?.Invoke(this, new("[create/cancelled]"));
            return;
        }

        Operation?.Invoke(this, new("[create/starting]"));

        await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        try
        {
            using (var client = new AdminClientBuilder(new AdminClientConfig
                   {
                       BootstrapServers = _consumerConfig.BootstrapServers
                   }).Build())
            {
                Operation?.Invoke(this, new("[create.get-metadata/starting]"));

                var metadata = client.GetMetadata(Topic, _operationTimeout);

                Operation?.Invoke(this, new("[create.get-metadata/completed]"));

                if (metadata == null)
                {
                    Operation?.Invoke(this, new("[create.create-topics/starting]"));

                    await client.CreateTopicsAsync(new[]
                    {
                        new TopicSpecification
                        {
                            Name = Topic,
                            ReplicationFactor = _kafkaOptions.ReplicationFactor,
                            NumPartitions = _kafkaOptions.NumPartitions
                        }
                    }).ConfigureAwait(false);

                    Operation?.Invoke(this, new("[create.create-topics/completed]"));
                }

                Operation?.Invoke(this, new("[create/completed]"));
            }
        }
        catch (OperationCanceledException)
        {
            Operation?.Invoke(this, new("[create/cancelled]"));
        }
        finally
        {
            _lock.Release();
        }
    }

    public void Dispose()
    {
        _lock.Wait(CancellationToken.None);

        try
        {
            if (_disposed)
            {
                return;
            }

            try
            {
                Operation?.Invoke(this, new("[dispose.producer.flush/starting]"));
                _producer.Flush(_operationTimeout);
                Operation?.Invoke(this, new("[dispose.producer.flush/completed]"));
            }
            catch (Exception ex)
            {
                // ignore
                Operation?.Invoke(this, new("[dispose.producer.flush/exception]", ex));
            }

            Operation?.Invoke(this, new("[dispose.producer.dispose/starting]"));
            _producer.Dispose();
            Operation?.Invoke(this, new("[dispose.producer.dispose/completed]"));

            try
            {
                Operation?.Invoke(this, new("[dispose.consumer.unsubscribe/starting]"));
                _consumer.Unsubscribe();
                Operation?.Invoke(this, new("[dispose.consumer.unsubscribe/completed]"));

                Operation?.Invoke(this, new("[dispose.consumer.close/starting]"));
                _consumer.Close();
                Operation?.Invoke(this, new("[dispose.consumer.close/completed]"));
            }
            catch
            {
                // ignore
            }

            Operation?.Invoke(this, new("[dispose.consumer.dispose/starting]"));
            _consumer.Dispose();
            Operation?.Invoke(this, new("[dispose.consumer.dispose/starting]"));

            _disposed = true;
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task DropAsync()
    {
        if (_cancellationToken.IsCancellationRequested)
        {
            Operation?.Invoke(this, new("[drop/cancelled]"));
            return;
        }

        Operation?.Invoke(this, new("[drop/starting]"));

        await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        try
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
                    await client.DeleteTopicsAsync(new List<string>
                    {
                        Topic
                    }, new() { OperationTimeout = _operationTimeout }).ConfigureAwait(false);
                }
                catch (DeleteTopicsException)
                {
                }
                catch (AggregateException ex) when (ex.InnerException is DeleteTopicsException)
                {
                }
            }
        }
        catch (OperationCanceledException)
        {
            Operation?.Invoke(this, new("[drop/cancelled]"));
        }
        finally
        {
            _lock.Release();
        }

        Operation?.Invoke(this, new("[drop/completed]"));
    }

    public async Task PurgeAsync()
    {
        Operation?.Invoke(this, new("[purge/starting]"));

        await DropAsync();
        await CreateAsync();

        Operation?.Invoke(this, new("[purge/completed]"));
    }

    public QueueUri Uri { get; }
    public bool IsStream => true;

    public async Task AcknowledgeAsync(object acknowledgementToken)
    {
        if (Guard.AgainstNull(acknowledgementToken) is not AcknowledgementToken token)
        {
            return;
        }

        await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        try
        {
            if (_disposed)
            {
                return;
            }

            if (!(_consumerConfig.EnableAutoCommit ?? false) &&
                !(_consumerConfig.EnableAutoOffsetStore ?? false))
            {
                if (!(_consumerConfig.EnableAutoCommit ?? false))
                {
                    _consumer.Commit(token.ConsumeResult);
                }

                if (!(_consumerConfig.EnableAutoOffsetStore ?? false))
                {
                    _consumer.StoreOffset(token.ConsumeResult);
                }
            }

            MessageAcknowledged?.Invoke(this, new(acknowledgementToken));
        }
        finally
        {
            _lock.Release();
        }
    }

    public async Task<ReceivedMessage?> GetMessageAsync()
    {
        await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        try
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

            var receivedMessage = _receivedMessages.Count > 0 ? _receivedMessages.Dequeue() : null;

            if (receivedMessage != null)
            {
                MessageReceived?.Invoke(this, new(receivedMessage));
            }

            return receivedMessage;
        }
        finally
        {
            _lock.Release();
        }
    }

    public async ValueTask<bool> IsEmptyAsync()
    {
        if (_cancellationToken.IsCancellationRequested)
        {
            Operation?.Invoke(this, new("[is-empty/cancelled]", true));
            return true;
        }

        Operation?.Invoke(this, new("[is-empty/starting]"));

        await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        try
        {
            if (_receivedMessages.Count > 0 || _disposed)
            {
                return false;
            }

            ReadMessage();

            var result = _receivedMessages.Count == 0;

            Operation?.Invoke(this, new("[is-empty]", result));

            return result;
        }
        catch (OperationCanceledException)
        {
            Operation?.Invoke(this, new("[is-empty/cancelled]", true));
        }
        finally
        {
            _lock.Release();
        }

        return true;
    }

    public async Task ReleaseAsync(object acknowledgementToken)
    {
        if (Guard.AgainstNull(acknowledgementToken) is not AcknowledgementToken token)
        {
            return;
        }

        await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        try
        {
            if (_disposed)
            {
                return;
            }

            _receivedMessages.Enqueue(new(new MemoryStream(Convert.FromBase64String(token.ConsumeResult.Message.Value)), acknowledgementToken));

            MessageReleased?.Invoke(this, new(acknowledgementToken));
        }
        finally
        {
            _lock.Release();
        }
    }

    public event EventHandler<MessageEnqueuedEventArgs>? MessageEnqueued;
    public event EventHandler<MessageAcknowledgedEventArgs>? MessageAcknowledged;
    public event EventHandler<MessageReleasedEventArgs>? MessageReleased;
    public event EventHandler<MessageReceivedEventArgs>? MessageReceived;
    public event EventHandler<OperationEventArgs>? Operation;

    public async Task EnqueueAsync(TransportMessage transportMessage, Stream stream)
    {
        Guard.AgainstNull(transportMessage, nameof(transportMessage));
        Guard.AgainstNull(stream, nameof(stream));

        await _lock.WaitAsync(CancellationToken.None).ConfigureAwait(false);

        try
        {
            if (_disposed)
            {
                return;
            }

            var value = Convert.ToBase64String(await stream.ToBytesAsync().ConfigureAwait(false));

            // always use `Produce` as `ProduceAsync` waits for the `DeliveryReport` to be produced, which slows down message sending
            _producer.Produce(Topic,
                new()
                {
                    Value = value
                });

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

            MessageEnqueued?.Invoke(this, new(transportMessage, stream));
        }
        catch (OperationCanceledException)
        {
        }
        finally
        {
            _lock.Release();
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

        ConsumeResult<Ignore, string>? consumeResult = null;

        try
        {
            consumeResult = _kafkaOptions.UseCancellationToken ? _consumer.Consume(_cancellationToken) : _consumer.Consume(_kafkaOptions.ConsumeTimeout);
        }
        catch (OperationCanceledException)
        {
        }

        if (consumeResult == null)
        {
            return;
        }

        var acknowledgementToken = new AcknowledgementToken(Guid.NewGuid(), consumeResult);

        _receivedMessages.Enqueue(new(new MemoryStream(Convert.FromBase64String(consumeResult.Message.Value)), acknowledgementToken));
    }

    internal class AcknowledgementToken
    {
        public AcknowledgementToken(Guid messageId, ConsumeResult<Ignore, string> consumeResult)
        {
            MessageId = messageId;
            ConsumeResult = consumeResult;
        }

        public ConsumeResult<Ignore, string> ConsumeResult { get; }

        public Guid MessageId { get; }
    }
}