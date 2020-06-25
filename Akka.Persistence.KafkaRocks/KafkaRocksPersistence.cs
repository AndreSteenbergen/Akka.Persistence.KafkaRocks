using Akka.Actor;
using Akka.Configuration;

namespace Akka.Persistence.KafkaRocks
{
    //https://github.com/AkkaNetContrib/Akka.Persistence.RocksDb/blob/dev/src/Akka.Persistence.RocksDb/reference.conf
    public class KafkaRocksPersistence: IExtension
    {
        public static KafkaRocksPersistence Get(ActorSystem system) => system.WithExtension<KafkaRocksPersistence, KafkaRocksPersistenceProvider>();
        public static Config DefaultConfig() => ConfigurationFactory.FromResource<KafkaRocksPersistence>("Akka.Persistence.KafkaRocks.reference.conf");
        
        public KafkaRocksJournalSettings JournalSettings { get; }
        
        public KafkaRocksPersistence(ExtendedActorSystem system)
        {
            system.Settings.InjectTopLevelFallback(DefaultConfig());
            JournalSettings = KafkaRocksJournalSettings.Create(
                system.Settings.Config.GetConfig("akka.persistence.journal.kafkarocks"),
                system.Serialization);
        }
    }
}