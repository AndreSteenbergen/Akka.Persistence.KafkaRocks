using System;
using Akka.Actor;

namespace Akka.Persistence.KafkaRocks.Query
{
    public class EventsByPersistenceIdPublisher
    {
        public sealed class Continue
        {
            public static Continue Instance { get; } = new Continue();
            private Continue() { }
        }

        public static Props Props(string persistenceId, long fromSequenceNr, long toSequenceNr, TimeSpan? refreshInterval, int maxBufferSize, string writeJournalPluginId)
        {
            return refreshInterval.HasValue
                ? Actor.Props.Create(() => new LiveEventsByPersistenceIdPublisher(persistenceId, fromSequenceNr, toSequenceNr, maxBufferSize, writeJournalPluginId, refreshInterval.Value))
                : Actor.Props.Create(() => new CurrentEventsByPersistenceIdPublisher(persistenceId, fromSequenceNr, toSequenceNr, maxBufferSize, writeJournalPluginId));
        }
    }
}