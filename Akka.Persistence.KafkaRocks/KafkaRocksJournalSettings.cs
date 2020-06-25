using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Akka.Configuration.Hocon;
using Confluent.Kafka;
using Config = Akka.Configuration.Config;

namespace Akka.Persistence.KafkaRocks
{
    public class RocksDbSettings
    {
        public RocksDbSettings(string path, bool checksum, bool fSync)
        {
            Path = path;
            Checksum = checksum;
            FSync = fSync;
        }

        public string Path { get; }

        public bool Checksum { get; }

        public bool FSync { get; }
        public static RocksDbSettings Create(Config config)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config));

            return new RocksDbSettings(
                path: config.GetString("path"),
                checksum: config.GetBoolean("checksum"),
                fSync: config.GetBoolean("fsync"));
        }
    }
    
    public class KafkaRocksJournalSettings
    {
        public KafkaRocksJournalSettings(
            string eventTopicMapperClassname,
            Config kafkaConfig,
            Akka.Serialization.Serialization serialization,
            IEnumerable<TopicPartition> enabledTopicPartitions,
            RocksDbSettings rocksDbSettings)
        {
            KafkaConfig = kafkaConfig;
            EventTopicMapper = (IEventTopicMapper) Activator.CreateInstance(Type.GetType(eventTopicMapperClassname) ?? throw new InvalidOperationException());
            Serialization = serialization;
            RocksDbSettings = rocksDbSettings;
            EnabledTopicPartitions = enabledTopicPartitions?.ToImmutableArray() ?? ImmutableArray<TopicPartition>.Empty;
        }

        public ImmutableArray<TopicPartition> EnabledTopicPartitions { get; }
        public IEventTopicMapper EventTopicMapper { get; }
        public Config KafkaConfig { get; }
        public Akka.Serialization.Serialization Serialization { get; }
        public RocksDbSettings RocksDbSettings { get; }

        public static KafkaRocksJournalSettings Create(Config config, Akka.Serialization.Serialization serialization)
        {
            if (config == null)
                throw new ArgumentNullException(nameof(config));
            
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

            return new KafkaRocksJournalSettings(
                config.GetString("eventTopicMapperClassname"),
                config.GetConfig("kafka"),
                serialization,
                tpos,
                RocksDbSettings.Create(config.GetConfig("rocksdb")));
        }
    } 
}