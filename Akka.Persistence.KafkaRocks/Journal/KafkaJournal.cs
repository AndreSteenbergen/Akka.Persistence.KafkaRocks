using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Persistence.Journal;
using Akka.Streams;
using Akka.Streams.Kafka.Dsl;
using Akka.Streams.Kafka.Settings;
using Akka.Util.Internal;
using Confluent.Kafka;
using RocksDbSharp;
using static Akka.Persistence.KafkaRocks.Journal.RocksDbKey;

namespace Akka.Persistence.KafkaRocks.Journal
{
    /// <summary>
    /// Journal based on kafka events stream, buffered through a rocksdb no sql eventstore, keeping a single source of truth (kafka) with the
    /// benefits of fast retrieval of events by persistenceId. All applications reading from Kafka should only need to subscribe to
    /// events in the kafka streams.
    /// </summary>
    public class KafkaJournal : AsyncWriteJournal
    {
        private readonly KafkaRocksJournalSettings settings;
        private ActorMaterializer materializer;
        private IProducer<string, byte[]> producer;
        
        private IEventTopicMapper topicMapper;

        private readonly DbOptions rocksDbOptions = new DbOptions().SetCreateIfMissing(true);
        private ReadOptions rocksDbReadOptions;
        private WriteOptions rocksDbWriteOptions;
        private RocksDb database;

        private const int partitionIdOffset = 10;
        private const int idOffset = 100;
        
        private Dictionary<string, int> idMap = new Dictionary<string, int>();
        
        private Dictionary<TopicPartition, int> topicPartitionMap = new Dictionary<TopicPartition, int>();
        private Dictionary<TopicPartition, long> currentOffsets = new Dictionary<TopicPartition, long>();

        private readonly object idMapLock = new object();
        private readonly object topicPartitionMapLock = new object();
        
        private readonly HashSet<IActorRef> allPersistenceIdsSubscribers = new HashSet<IActorRef>();

        private readonly Dictionary<string, HashSet<IActorRef>> persistenceIdSubscribers =
            new Dictionary<string, HashSet<IActorRef>>();

        private readonly Dictionary<string, HashSet<IActorRef>> tagSubscribers = new Dictionary<string, HashSet<IActorRef>>();
        private HashSet<TopicPartition> enabledTopicPartitions;
        
        private HashSet<TopicPartition> eofsFound = new HashSet<TopicPartition>();
        private readonly Dictionary<TopicPartition, List<TaskCompletionSource<bool>>> eofListeners = new Dictionary<TopicPartition, List<TaskCompletionSource<bool>>>();
        private TaskCompletionSource<bool> allEofsFound = new TaskCompletionSource<bool>();

        private readonly Dictionary<string, long> tagSequenceNrs = new Dictionary<string, long>();

        private const string TagPersistenceIdPrefix = "$$$";

        public KafkaJournal()
        {
            settings = KafkaRocksPersistence.Get(Context.System).JournalSettings;
        }

        protected override void PreStart()
        {
#if DEBUG
            Console.WriteLine("PreStart");            
#endif
            base.PreStart();

            var producerSettings = ProducerSettings<string, byte[]>
                .Create(settings.KafkaConfig, Serializers.Utf8, Serializers.ByteArray)
                .WithBootstrapServers(settings.KafkaConfig.GetString("bootstrap.servers"));

            materializer = Context.Materializer();

            producer = producerSettings.CreateKafkaProducer();
            topicMapper = settings.EventTopicMapper;
            enabledTopicPartitions = new HashSet<TopicPartition>(settings.EnabledTopicPartitions);

#if DEBUG
            Console.WriteLine("Gettings RocksDB");            
#endif
            var rocksDbSettings = settings.RocksDbSettings;
            rocksDbReadOptions = new ReadOptions().SetVerifyChecksums(rocksDbSettings.Checksum);
            rocksDbWriteOptions = new WriteOptions().SetSync(rocksDbSettings.FSync);

            //retrieve offsets for the consumer to start reading; as rocksdb possible already knows about
            database = RocksDb.Open(rocksDbOptions, rocksDbSettings.Path);

#if DEBUG
            Console.WriteLine("Database opened");            
#endif            
            idMap = ReadIdMap();
            currentOffsets = ReadOffsetsForPartitions();
        
            StartConsuming();
        }

        private void StartConsuming()
        {
#if DEBUG
    Console.WriteLine("Start Consuming");            
#endif
            allEofsFound = new TaskCompletionSource<bool>();
            
            //use eof to know when we are ready with a single topic partition
            var consumerSettings = ConsumerSettings<string, byte[]>
                .Create(settings.KafkaConfig, Deserializers.Utf8, Deserializers.ByteArray)
                .WithBootstrapServers(settings.KafkaConfig.GetString("bootstrap.servers"))
                .WithGroupId(settings.KafkaConfig.GetString("groupid.prefix") + Guid.NewGuid())
                .WithProperty("enable.partition.eof", "true");

            var adminClientBuilder = new AdminClientBuilder(consumerSettings.Properties);

            Metadata metadata = null;
            using (var adminClient = adminClientBuilder.Build())
            {
                metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
            }

            if (metadata == null) throw new Exception("can not retrieve metadata from bootstrap servers");
            var knownTopicPartitions = new HashSet<TopicPartition>();
            foreach (var metadataTopic in metadata.Topics)
            {
                foreach (var metadataTopicPartition in metadataTopic.Partitions)
                {
                    knownTopicPartitions.Add(new TopicPartition(metadataTopic.Topic, metadataTopicPartition.PartitionId));
                }
            }

            var topicsNotInKafka = 0;
            //create a consumer for all enabled partition topics
            var tposForSubscription = new List<TopicPartitionOffset>();
            foreach (var tp in enabledTopicPartitions)
            {
                if (knownTopicPartitions.Contains(tp))
                {
                    tposForSubscription.Add(currentOffsets.TryGetValue(tp, out long offset)
                        ? new TopicPartitionOffset(tp, offset + 1)
                        : new TopicPartitionOffset(tp, Offset.Beginning));
                }
                else
                {
                    //if not present in kafka, then we know we are at the end
                    eofsFound.Add(tp);
                    topicsNotInKafka++;
                }
            }
            
            if (tposForSubscription.Count == 0)
            {
                //empty start
                allEofsFound.SetResult(true);
                return;
            }
            
            var subscription = Subscriptions.AssignmentWithOffset(tposForSubscription.ToArray());
            var source = KafkaConsumer.PlainSource(consumerSettings, subscription);
            
            var writeBatches = new Dictionary<TopicPartition, WriteBatch>();
            eofsFound = new HashSet<TopicPartition>();
            
            //normally this won't take long. The topicpartition progress is also stored in rocksdb
            //if we need to reread all events then this can take a while 
            var cts = new CancellationTokenSource();

            var sourceTask = source
                .Via(cts.Token.AsFlow<ConsumeResult<string,byte[]>>(true))
                .RunForeach(msg =>
            {
                if (msg.IsPartitionEOF)
                {
                    //save as this is during startup; no-one can touch this topic partition yet.
                    currentOffsets[msg.TopicPartition] = msg.Offset;
                    
                    eofsFound.Add(msg.TopicPartition);
                    if (writeBatches.TryGetValue(msg.TopicPartition, out var b))
                    {
                        database.Write(b, rocksDbWriteOptions);
                        b.Dispose();
                        writeBatches.Remove(msg.TopicPartition);
                    }

                    if (eofListeners.TryGetValue(msg.TopicPartition, out var listeners))
                    {
                        foreach (var taskCompletionSource in listeners) taskCompletionSource.SetResult(true);
                        eofListeners.Remove(msg.TopicPartition);
                    }

                    if (eofsFound.Count != tposForSubscription.Count + topicsNotInKafka) return;
                    
                    allEofsFound.SetResult(true);
                    cts.Cancel();
                    cts.Dispose();
                    
                    return;
                }
                if (eofsFound.Contains(msg.TopicPartition)) return;
                
                // presume we are the only one writing to this topic partition (otherwise akka persistence gets messy real quick)
                if (!writeBatches.TryGetValue(msg.TopicPartition, out var writebatch))
                {
                    writeBatches[msg.TopicPartition] = writebatch = new WriteBatch();
                }

                //add event to the writebatch
                var persistent = PersistentFromMessage(msg);
                WriteToRocksDbBatch(persistent, writebatch);
                
                //store current offset into (using the writebatch)
                var key = TopicPartitionKey(TopicPartitionNumericId(msg.TopicPartition));
                writebatch.Put(KeyToBytes(key), CounterToBytes(msg.Offset));

            }, materializer);
            
            sourceTask.ContinueWith(x =>
            {
                Console.WriteLine("Source stopped for kafka journal");
            });
        }
        
        protected override void PostStop()
        {
#if DEBUG
            Console.WriteLine("PostStop called");            
#endif
            database?.Dispose();
            
            producer.Flush();
            producer.Dispose();
            
            base.PostStop();
        }

        protected override bool ReceivePluginInternal(object message)
        {
            message.Match()
                .With<ReplayTaggedMessages>(rtm =>
                {
                    var readHighestSequenceNrFrom = Math.Max(0L, rtm.FromSequenceNr - 1);
                    ReadHighestSequenceNrAsync(TagAsPersistenceId(rtm.Tag), readHighestSequenceNrFrom)
                        .ContinueWith(task =>
                        {
                            var highSeqNr = task.Result;
                            var toSeqNr = Math.Min(rtm.ToSequenceNr, highSeqNr);
                            if (highSeqNr == 0L || rtm.FromSequenceNr > toSeqNr)
                            {
                                return highSeqNr;
                            }
                            else
                            {
                                var res = ReplayTaggedMessagesAsync(rtm.Tag, rtm.FromSequenceNr, toSeqNr, rtm.Max, taggedMessage =>
                                {
                                    AdaptFromJournal(taggedMessage.Persistent).ForEach(adaptedPersistentRepr =>
                                    {
                                        rtm.ReplyTo.Tell(
                                            new ReplayedTaggedMessage(adaptedPersistentRepr, rtm.Tag, taggedMessage.Offset),
                                            ActorRefs.NoSender);
                                    });
                                }).ContinueWith(t => highSeqNr, TaskContinuationOptions.OnlyOnRanToCompletion);

                                return res.Result;
                            }
                        }).ContinueWith<IJournalResponse>(task =>
                        {
                            if (task.IsCompleted)
                                return new RecoverySuccess(task.Result);
                            else
                                return new ReplayMessagesFailure(task.Exception);
                        }).PipeTo(rtm.ReplyTo);
                })
                .With<SubscribePersistenceId>(subscribePersistenceId =>
                {
                    AddPersistenceIdSubscriber(Sender, subscribePersistenceId.PersistenceId);
                    Context.Watch(Sender);
                })
                .With<SubscribeAllPersistenceIds>(_ =>
                {
                    AddAllPersistenceIdsSubscriber(Sender);
                    Context.Watch(Sender);
                })
                .With<SubscribeTag>(subscribeTag =>
                {
                    AddTagSubscriber(Sender, subscribeTag.Tag);
                    Context.Watch(Sender);
                })
                .With<Terminated>(t => RemoveSubscriber(t.ActorRef));

            return true;
        }

        public override async Task ReplayMessagesAsync(IActorContext context, string persistenceId, long fromSequenceNr,
            long toSequenceNr, long max,
            Action<IPersistentRepresentation> recoveryCallback)
        {
            var topicPartition = topicMapper.GetTopicPartitionForPersistenceId(persistenceId);
            if (topicPartition.Equals(default(TopicPartition))) throw new Exception("PersistanceId not mapped to TopicPartition");
            
            if (!eofsFound.Contains(topicPartition))
            {
                await (WaitForEofInTopicPartition(topicPartition));
            }
            
            var nid = NumericId(persistenceId);
            ReplayMessages(nid, fromSequenceNr, toSequenceNr, max, recoveryCallback);
        }

        private void ReplayMessages(
            int persistenceId,
            long fromSequenceNr,
            long toSequenceNr,
            long max,
            Action<IPersistentRepresentation> recoveryCallback)
        {
            void Go(Iterator iter, Key key, long ctr, Action<IPersistentRepresentation> replayCallback)
            {
                if (iter.Valid())
                {
                    var nextEntry = iter.PeekAndNext();
                    var nextKey = KeyFromBytes(nextEntry.Key);
                    if (nextKey.SequenceNr > toSequenceNr)
                    {
                        // end iteration here
                    }
                    else if (IsDeletionKey(nextKey))
                    {
                        // this case is needed to discard old events with deletion marker
                        Go(iter, nextKey, ctr, replayCallback);
                    }
                    else if (key.PersistenceId == nextKey.PersistenceId)
                    {
                        var msg = PersistentFromBytes(nextEntry.Value);
                        var del = Deletion(iter, nextKey);
                        if (ctr < max)
                        {
                            if (!del)
                                replayCallback(msg);
                            Go(iter, nextKey, ctr + 1L, replayCallback);
                        }
                    }
                }
            }

            // need to have this to be able to read journal created with 1.0.x, which
            // supported deletion of individual events
            bool Deletion(Iterator iter, Key key)
            {
                if (iter.Valid())
                {
                    var nextEntry = iter.Peek();
                    var nextKey = KeyFromBytes(nextEntry.Key);
                    if (key.PersistenceId == nextKey.PersistenceId && key.SequenceNr == nextKey.SequenceNr &&
                        IsDeletionKey(nextKey))
                    {
                        iter.Next();
                        return true;
                    }
                    else return false;
                }
                else
                {
                    return false;
                }
            }

            WithIterator<NotUsed>(iter =>
            {
                var startKey = new Key(persistenceId, fromSequenceNr < 1L ? 1L : fromSequenceNr, 0);
                iter.Seek(KeyToBytes(startKey));
                Go(iter, startKey, 0L, recoveryCallback);

                return NotUsed.Instance;
            });
        }
        
        private async Task<bool> WaitForEofInTopicPartition(TopicPartition topicPartition)
        {
            var tcs = new TaskCompletionSource<bool>();
            if (!eofListeners.TryGetValue(topicPartition, out var list))
            {
                eofListeners[topicPartition] = list = new List<TaskCompletionSource<bool>>();
            }
            list.Add(tcs);
            return await tcs.Task;
        }
        
        public override async Task<long> ReadHighestSequenceNrAsync(string persistenceId, long fromSequenceNr)
        {
            var topicPartition = topicMapper.GetTopicPartitionForPersistenceId(persistenceId);
            if (topicPartition.Equals(default(TopicPartition))) throw new Exception("PersistanceId not mapped to TopicPartition");
            
            if (!eofsFound.Contains(topicPartition))
            {
                await (WaitForEofInTopicPartition(topicPartition));
            }
        
            var nid = NumericId(persistenceId);
            return ReadHighestSequenceNr(nid);
        }

        private long ReadHighestSequenceNr(int persistenceId)
        {
            var ro = RocksDbSnapshot();
            try
            {
                var num = database.Get(KeyToBytes(CounterKey(persistenceId)), cf: null, readOptions: ro);
                return num == null ? 0L : CounterFromBytes(num);
            }
            finally
            {
                //ro.Snapshot().Dispose();
            }
        }

        private async Task ReplayTaggedMessagesAsync(
            string tag,
            long fromSequenceNr,
            long toSequenceNr,
            long max,
            Action<ReplayedTaggedMessage> recoveryCallback)
        {
            await allEofsFound.Task;
            
            var tagNid = TagNumericId(tag);
            ReplayTaggedMessages(tag, tagNid, fromSequenceNr, toSequenceNr, max, recoveryCallback);
        }
        
        private void ReplayTaggedMessages(
            string tag,
            int tagInt,
            long fromSequenceNr,
            long toSequenceNr,
            long max,
            Action<ReplayedTaggedMessage> recoveryCallback)
        {
            void Go(Iterator iter, Key key, long ctr, Action<ReplayedTaggedMessage> replayCallback)
            {
                if (iter.Valid())
                {
                    var nextEntry = iter.PeekAndNext();
                    var nextKey = KeyFromBytes(nextEntry.Key);
                    if (nextKey.SequenceNr > toSequenceNr)
                    {
                        // end iteration here
                    }
                    else if (key.PersistenceId == nextKey.PersistenceId)
                    {
                        var msg = PersistentFromBytes(nextEntry.Value);
                        if (ctr < max)
                        {
                            replayCallback(new ReplayedTaggedMessage(msg, tag, nextKey.SequenceNr));
                            Go(iter, nextKey, ctr + 1L, replayCallback);
                        }
                    }
                }
            }

            WithIterator(iter =>
            {
                var startKey = new Key(tagInt, fromSequenceNr < 1L ? 1L : fromSequenceNr + 1, 0);
                iter.Seek(KeyToBytes(startKey));
                Go(iter, startKey, 0L, recoveryCallback);

                return NotUsed.Instance;
            });
        }
        
        protected override async Task<IImmutableList<Exception>> WriteMessagesAsync(IEnumerable<AtomicWrite> messages)
        {
            var persistenceIds = new HashSet<string>();
            var allTags = new HashSet<string>();
            
            var writeTasks = new List<Task>();
            
            foreach (AtomicWrite atomicWrite in messages)
            {
                //create rocksdb batch
                var writebatch = new WriteBatch();
                var topicPartitionsInBatch = new HashSet<TopicPartition>();
                
                var atomicWriteTasks = new List<Task>();
                var mappedPartition = topicMapper.GetTopicPartitionForPersistenceId(atomicWrite.PersistenceId);

                if (!eofsFound.Contains(mappedPartition))
                {
                    await WaitForEofInTopicPartition(mappedPartition);
                }
                
                //we can write messages more then once; this is no problem, because the sequence is always present in the header
                //not likely to write the same data more then once, only in test
                if (mappedPartition.Equals(default(TopicPartition))) throw new Exception("PersistanceId not mapped to TopicPartition");
                
                var payloads = atomicWrite.Payload.AsInstanceOf<ImmutableList<IPersistentRepresentation>>();
                foreach (var payload in payloads)
                {
                    var kafkaMsg = MessageFromIPersistentRepresentation(payload);
                    var produceTask = producer
                        .ProduceAsync(mappedPartition, kafkaMsg)
                        .ContinueWith(msgTask =>
                        {
                            lock (topicPartitionMapLock)
                            {
                                var msg = msgTask.Result;    
                                //store current offset into (using the writebatch)
                                if (currentOffsets.TryGetValue(msg.TopicPartition, out long offset))
                                {
                                    if (offset < msg.Offset) currentOffsets[msg.TopicPartition] = msg.Offset;
                                }
                                else
                                {
                                    currentOffsets[msg.TopicPartition] = msg.Offset;
                                }

                                topicPartitionsInBatch.Add(msg.TopicPartition);
                            }

                            WriteToRocksDbBatch(payload, writebatch);
                            if (tagSubscribers.Count > 0 && (payload.Payload is Tagged tagged))
                            {
                                allTags = allTags.Union(tagged.Tags).ToHashSet();
                            }

                            if (persistenceIdSubscribers.Count > 0)
                            {
                                persistenceIds.Add(atomicWrite.PersistenceId);
                            }
                        }, TaskContinuationOptions.NotOnFaulted | TaskContinuationOptions.NotOnFaulted);
                        
                    atomicWriteTasks.Add(produceTask);
                }
                writeTasks.Add(Task.WhenAll(atomicWriteTasks).ContinueWith(t =>
                {
                    foreach (var topicPartition in topicPartitionsInBatch)
                    {
                        var key = TopicPartitionKey(TopicPartitionNumericId(topicPartition));
                        writebatch.Put(KeyToBytes(key), CounterToBytes(currentOffsets[topicPartition]));    
                    }

                    database.Write(writebatch);
                    writebatch.Dispose();
                }));
            }

            var result = await Task.WhenAll(writeTasks).ContinueWith(_ =>
                {
                    var rst = writeTasks
                        .Select(t => t.IsFaulted ? TryUnwrapException(t.Exception) : null)
                        .ToImmutableList() as IImmutableList<Exception>;

                    if (persistenceIds.Count > 0)
                    {
                        persistenceIds.ForEach(NotifyPersistenceIdChange);
                    }
                    if (allTags.Count > 0)
                    {
                        allTags.ForEach(NotifyTagChange);
                    }
                    
                    return rst;
                }
            );
            return result;
        }

        protected override Task DeleteMessagesToAsync(string persistenceId, long toSequenceNr)
        {
            //TODO: for an example in Rocksdb only: https://github.com/AkkaNetContrib/Akka.Persistence.RocksDb/blob/dev/src/Akka.Persistence.RocksDb/Journal/RocksDbJournal.cs
            //add a deleteTo message in Kafka
            throw new NotImplementedException();
        }

        private ReadOptions RocksDbSnapshot() => rocksDbReadOptions.SetSnapshot(database.CreateSnapshot());

        private T WithIterator<T>(Func<Iterator, T> body)
        {
            var ro = RocksDbSnapshot();
            var iterator = database.NewIterator(cf: null, readOptions: ro);
            try
            {
                return body(iterator);
            }
            finally
            {
                iterator.Dispose();
            }
        }

        /// <summary>
        /// Get the mapped numeric id for the specified persistent actor <paramref name="id"/>. Creates and
        /// stores a new mapping if necessary.
        /// </summary>
        private int NumericId(string id)
        {
            lock (idMapLock)
            {
                var result = idMap.TryGetValue(id, out int v) ? v : WriteIdMapping(id, idMap.Count + idOffset);
                return result;
            }
        }

        private int TopicPartionNumeridId(TopicPartition id)
        {
            lock (topicPartitionMapLock)
            {
                var result = topicPartitionMap.TryGetValue(id, out int v) ? v : WriteTopicPartitionMapping(id, topicPartitionMap.Count + partitionIdOffset);
                return result;
            }
        }

        private int WriteTopicPartitionMapping(TopicPartition topicPartition, int numericId)
        {
            topicPartitionMap.Add(topicPartition, numericId);
            database.Put(KeyToBytes(MappingKey(numericId)), FromTopicPartition(topicPartition));
            return numericId;
        }
        
        private int WriteIdMapping(string id, int numericId)
        {
            idMap.Add(id, numericId);
            database.Put(KeyToBytes(MappingKey(numericId)), Encoding.UTF8.GetBytes(id));
            NewPersistenceIdAdded(id);
            return numericId;
        }

        private void NotifyPersistenceIdChange(string persistenceId)
        {
            if (persistenceIdSubscribers.TryGetValue(persistenceId, out var subscribers))
            {
                var changed = new EventAppended(persistenceId);
                subscribers.ForEach(s => s.Tell(changed));
            }
        }

        private void NotifyTagChange(string tag)
        {
            if (tagSubscribers.TryGetValue(tag, out var subscribers))
            {
                var changed = new TaggedEventAppended(tag);
                subscribers.ForEach(s => s.Tell(changed));
            }
        }
        
        private void NewPersistenceIdAdded(string id)
        {
            if (id.StartsWith(TagPersistenceIdPrefix)) return;

            var added = new PersistenceIdAdded(id);
            foreach (var actorRef in allPersistenceIdsSubscribers)
            {
                actorRef.Tell(added);
            }
        }

        private Dictionary<string, int> ReadIdMap() => WithIterator(iter =>
        {
            iter.Seek(KeyToBytes(MappingKey(idOffset)));
            var result =  ReadIdMap(new Dictionary<string, int>(), iter);
            return result;
        });

        private Dictionary<string, int> ReadIdMap(Dictionary<string, int> pathMap, Iterator iter)
        {
            if (!iter.Valid())
            {
                return pathMap;
            }

            var entryKey = KeyFromBytes(iter.Key());
            if (!IsMappingKey(entryKey))
            {
                return pathMap;
            }
            
            var value = Encoding.UTF8.GetString(iter.Value());
            iter.Next();
            
            //only return the persistenceId's we know hot to map to topic partition
            return topicMapper.GetTopicPartitionForPersistenceId(value).Equals(default(TopicPartition))
                ? ReadIdMap(pathMap, iter)
                : ReadIdMap(new Dictionary<string, int>(pathMap) {[value] = entryKey.MappingId}, iter);
        }

        private Dictionary<TopicPartition, long> ReadOffsetsForPartitions() => WithIterator(iter =>
        {
            //begin met de mapping key om int => partitionTopic te komen
            iter.Seek(KeyToBytes(MappingKey(partitionIdOffset)));
            topicPartitionMap = ReadPartitionTopicIds(new Dictionary<TopicPartition, int>(), iter);
            
            var result = new Dictionary<TopicPartition, long>();
            foreach (KeyValuePair<TopicPartition, int> topicPartitionId in topicPartitionMap)
            {
                var longBytes = database.Get(KeyToBytes(TopicPartitionKey(topicPartitionId.Value)));
                result[topicPartitionId.Key] = CounterFromBytes(longBytes);
            }

            return result;
        });
        
        private Dictionary<TopicPartition, int> ReadPartitionTopicIds(Dictionary<TopicPartition, int> pathMap, Iterator iter)
        {
            //in the id map we know the id => topic partition, but importantly PREFIX/TOPIC/PARTITION
            if (!iter.Valid())
            {
                return pathMap;
            }
            
            var entryKey = KeyFromBytes(iter.Key());
            if (!IsMappingKey(entryKey) || entryKey.MappingId >= idOffset)
            {
                return pathMap;
            }
            
            var value = TopicPartitionFromBytes(iter.Value());
            iter.Next();
            return ReadPartitionTopicIds(new Dictionary<TopicPartition, int>(pathMap) {[value] = entryKey.MappingId}, iter);
        }

        private int TagNumericId(string tag) => NumericId(TagAsPersistenceId(tag));
        private int TopicPartitionNumericId(TopicPartition topicPartition) => TopicPartionNumeridId(topicPartition);
        
        private string TagAsPersistenceId(string tag) => TagPersistenceIdPrefix + tag;
        
        private void AddPersistenceIdSubscriber(IActorRef subscriber, string persistenceId)
        {
            persistenceIdSubscribers.AddBinding(persistenceId, subscriber);
        }

        private void RemoveSubscriber(IActorRef subscriber)
        {
            foreach (var kv in persistenceIdSubscribers)
            {
                persistenceIdSubscribers.RemoveBinding(kv.Key, subscriber);
            }

            foreach (var kv in tagSubscribers)
            {
                tagSubscribers.RemoveBinding(kv.Key, subscriber);
            }

            allPersistenceIdsSubscribers.Remove(subscriber);
        }

        private IPersistentRepresentation PersistentFromMessage(ConsumeResult<string, byte[]> message)
        {
            var headers = message.Message.Headers.ToDictionary(x => x.Key, x => x.GetValueBytes());
            
            var persistenceId = message.Message.Key;
            var eventTypeString = Encoding.UTF8.GetString(headers["clrType"]);
            var tagset = new HashSet<string>();
            if (headers.TryGetValue("tags", out byte[] tags))
            {
                var serializerForHashSet = settings.Serialization.FindSerializerForType(typeof(HashSet<string>));
                tagset = serializerForHashSet.FromBinary<HashSet<string>>(tags);
            }
            
            var eventType = Type.GetType(eventTypeString, true, true);

            var serializer = settings.Serialization.FindSerializerForType(eventType);
            var @event = serializer.FromBinary(message.Message.Value, eventType);

            return new Persistent(
                tagset?.Count > 0 ? new Tagged(@event, tagset) : @event,
                persistenceId: persistenceId,
                sequenceNr: CounterFromBytes(headers["sequenceNr"]));
        }

        private Message<string, byte[]> MessageFromIPersistentRepresentation(IPersistentRepresentation persistent)
        {
            var @event = persistent.Payload;
            var eventType = @event.GetType();
            var clrEventType = string.Concat(eventType.FullName, ", ", eventType.GetTypeInfo().Assembly.GetName().Name);
            
            var serializer = settings.Serialization.FindSerializerForType(eventType);
            
            var tags = new byte[0];
            if (persistent.Payload is Tagged tagged)
            {
                var serializerForHashSet = settings.Serialization.FindSerializerForType(typeof(HashSet<string>));
                tags = serializerForHashSet.ToBinary(tagged.Tags.ToHashSet());
            }
            
            var msg = new Message<string, byte[]>
            {
                Timestamp = Timestamp.Default,
                Key = persistent.PersistenceId,
                Value = serializer.ToBinary(@event),
                Headers = new Headers
                {
                    {"clrType", Encoding.UTF8.GetBytes(clrEventType)},
                    {"tags", tags},
                    {"sequenceNr", CounterToBytes(persistent.SequenceNr)}
                }
            };
            
            return msg;
        }
        
        private byte[] PersistentToBytes(IPersistentRepresentation message)
        {
            var serializer = settings.Serialization.FindSerializerForType(typeof(IPersistentRepresentation));
            return serializer.ToBinary(message);
        }
        
        private IPersistentRepresentation PersistentFromBytes(byte[] bytes)
        {
            var serializer = settings.Serialization.FindSerializerForType(typeof(IPersistentRepresentation));
            return serializer.FromBinary<IPersistentRepresentation>(bytes);
        }
        
        private void WriteToRocksDbBatch(
            IPersistentRepresentation persistent,
            WriteBatch batch)
        {
            //be aware the sequence number in the persistent is the offset of the message;
            var persistentBytes = PersistentToBytes(persistent);
            var nid = NumericId(persistent.PersistenceId);
            
            batch.Put(KeyToBytes(CounterKey(nid)), CounterToBytes(persistent.SequenceNr));
            batch.Put(KeyToBytes(new Key(nid, persistent.SequenceNr, 0)), persistentBytes);
            
            if (persistent.Payload is Tagged tagged)
            {
                tagged.Tags.ForEach(tag =>
                {
                    var tagNid = TagNumericId(tag);
                    var tagSequenceNr = NextTagSequenceNr(tag);
                    batch.Put(KeyToBytes(CounterKey(tagNid)), CounterToBytes(tagSequenceNr));
                    batch.Put(KeyToBytes(new Key(tagNid, tagSequenceNr, 0)), persistentBytes);
                });
            }
        }
        
        private long NextTagSequenceNr(string tag)
        {
            long n;
            if (!tagSequenceNrs.TryGetValue(tag, out n))
            {
                n = ReadHighestSequenceNr(TagNumericId(tag));
            }
            tagSequenceNrs[tag] = n + 1;
            return n + 1;
        }
        
        private void AddAllPersistenceIdsSubscriber(IActorRef subscriber)
        {
            allPersistenceIdsSubscribers.Add(subscriber);
            subscriber.Tell(new CurrentPersistenceIds(AllPersistenceIds()));
        }
        
        private void AddTagSubscriber(IActorRef subscriber, string tag)
        {
            tagSubscribers.AddBinding(tag, subscriber);
        }
        
        private HashSet<string> AllPersistenceIds()
        {
            lock (idMapLock)
            {
                return idMap.Keys.ToHashSet();
            }
        }
    }
}