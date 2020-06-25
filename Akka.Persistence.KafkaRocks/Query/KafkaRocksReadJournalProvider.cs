using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence.Query;

namespace Akka.Persistence.KafkaRocks.Query
{
    public class KafkaRocksReadJournalProvider : IReadJournalProvider
    {
        private readonly ExtendedActorSystem _system;
        private readonly Config _config;

        public KafkaRocksReadJournalProvider(ExtendedActorSystem system, Config config)
        {
            _system = system;
            _config = config;
        }

        public IReadJournal GetReadJournal()
        {
            return new KafkaRocksReadJournal(_system, _config);
        }
    }
}