using Confluent.Kafka;

namespace Akka.Persistence.KafkaRocks
{
    public interface IEventTopicMapper
    {
        TopicPartition GetTopicPartitionForPersistenceId(string persistenceId);
    }
}