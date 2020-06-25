using System;
using Akka.Configuration;
using Akka.Persistence.TCK.Journal;
using Xunit;
using Config = Akka.Configuration.Config;

namespace Akka.Persistence.KafkaRocks.Test
{
    [Collection("KafkaRocksSpec")]
    public class KafkaRocksJournalSpec : JournalSpec
    {
        //let's try always the same one ...
        private static Guid _testGuid = new Guid("08402333-833a-4770-bca5-ab2b7ab51784");
        static Config _instance;

        public static Guid TestGuid => _testGuid != Guid.Empty ? _testGuid : (_testGuid = Guid.NewGuid());
        public static Config SpecConfig => _instance ??= ConfigurationFactory.ParseString($@"
                        akka.loglevel = INFO
                        akka.persistence.journal {{
                            plugin = ""akka.persistence.journal.kafkarocks""
                            journal-plugin-fallback.recovery-event-timeout = 300s
                            kafkarocks {{
                                class = ""Akka.Persistence.KafkaRocks.Journal.KafkaJournal, Akka.Persistence.KafkaRocks""
                                plugin-dispatcher = ""akka.actor.default-dispatcher""
                                eventTopicMapperClassname = ""Akka.Persistence.KafkaRocks.Test.EventTopicMapper, Akka.Persistence.KafkaRocks.Test""
                                enableForTopicPartitions = [
                                    {{topic: ""Test-{TestGuid}"", partition : 0 }},
                                ]
                                kafka {{
                                    bootstrap.servers = ""127.0.0.1:9092""
                                    groupid.prefix = ""test-journal""
                                }}
                                rocksdb {{
                                    path = ""rocksjournal-{TestGuid}""
                                }}
                            }}
                        }}
                        akka.test.single-expect-default = 300s")
            .WithFallback(KafkaRocksPersistence.DefaultConfig());
        
        public KafkaRocksJournalSpec() : base(SpecConfig)
        {
            Initialize();
        }
    }
}