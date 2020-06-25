using System;
using System.Text;
using Confluent.Kafka;

namespace Akka.Persistence.KafkaRocks.Journal
{
    public static class RocksDbKey
    {
        public static byte[] KeyToBytes(Key key)
        {
            var bytes = new byte[16];
            bytes.PutInt(key.PersistenceId);
            bytes.PutLong(key.SequenceNr, 4);
            bytes.PutInt(key.MappingId, 4 + 8);
            return bytes;
        }

        public static Key KeyFromBytes(byte[] bytes)
        {
            int persistenceId = ByteHelpers.GetInt(bytes, 0);
            long sequenceNr = ByteHelpers.GetLong(bytes, 4);
            int mappingId = ByteHelpers.GetInt(bytes, 4 + 8);
            return new Key(persistenceId, sequenceNr, mappingId);
        }

        public static byte[] FromTopicPartition(TopicPartition topicPartition)
        {
            var topicBytes = Encoding.UTF8.GetBytes(topicPartition.Topic);
            var bytes = new byte[topicBytes.Length + 4];
            bytes.PutInt(topicPartition.Partition.Value);
            topicBytes.CopyTo(bytes, 4);
            return bytes;
        }

        public static TopicPartition TopicPartitionFromBytes(byte[] bytes)
        {
            int partitionId = ByteHelpers.GetInt(bytes, 0);
            var topic = Encoding.UTF8.GetString(bytes, 4, bytes.Length - 4);
            return new TopicPartition(topic, partitionId);
        }

        public static Key CounterKey(int persistenceId) => new Key(persistenceId, 0L, 0);

        public static byte[] CounterToBytes(long ctr)
        {
            var bytes = new byte[8];
            bytes.PutLong(ctr);
            return bytes;
        }
        
        public static long CounterFromBytes(byte[] bytes)
        {
            return ByteHelpers.GetLong(bytes, 0);
        }

        public static Key MappingKey(int id) => new Key(1, 0L, id);
        public static Key TopicPartitionKey(int persistenceId) => new Key(persistenceId, -1L, 2);
        public static bool IsMappingKey(Key key) => key.PersistenceId == 1;
        public static bool IsTopicPartitionKey(Key key) => key.MappingId == 2 && key.SequenceNr == -1L;

        public static Key DeletionKey(int persistenceId, long sequenceNr) => new Key(persistenceId, sequenceNr, 1);
        public static bool IsDeletionKey(Key key) => key.MappingId == 1;
    }
}