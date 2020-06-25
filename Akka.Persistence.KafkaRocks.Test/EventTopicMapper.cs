using System.Collections.Generic;
using System.Linq;
using Akka.Configuration.Hocon;
using Confluent.Kafka;

namespace Akka.Persistence.KafkaRocks.Test
{
    /// <summary>
    /// simple event to topic mapper to always return the first configured tpo
    /// </summary>
    public class EventTopicMapper : IEventTopicMapper
    {
        private readonly TopicPartition tpo;

        public EventTopicMapper()
        {
            var config = KafkaRocksJournalSpec.SpecConfig.GetConfig("akka.persistence.journal.kafkarocks");
            var cfg = config.GetValue("enableForTopicPartitions");
            var tpoCfgs = cfg.Values.First() as HoconArray;

            var tpos = new List<TopicPartition>();
            foreach (var hoconValue in tpoCfgs)
            {
                var tpoCfg = hoconValue.GetObject();
                var topic = tpoCfg.Items["topic"].GetString();
                var partition = tpoCfg.Items["partition"].GetInt();
                
                tpos.Add(new TopicPartition(topic, new Partition(partition)));
            }

            tpo = tpos.First();
        }
        
        public TopicPartition GetTopicPartitionForPersistenceId(string persistenceId)
        {
            return tpo;
        }
    }
}