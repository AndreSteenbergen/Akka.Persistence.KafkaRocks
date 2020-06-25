using Akka.Actor;

namespace Akka.Persistence.KafkaRocks
{
    public class KafkaRocksPersistenceProvider : ExtensionIdProvider<KafkaRocksPersistence>
    {
        public override KafkaRocksPersistence CreateExtension(ExtendedActorSystem system)
        {
            return new KafkaRocksPersistence(system);
        }
    }
}