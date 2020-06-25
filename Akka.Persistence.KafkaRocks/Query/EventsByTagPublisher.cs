using System;
using Akka.Actor;

namespace Akka.Persistence.KafkaRocks.Query
{
    public class EventsByTagPublisher
    {
        public sealed class Continue
        {
            public static Continue Instance { get; } = new Continue();
            private Continue() { }
        }

        public static Props Props(string tag, long fromOffset, long toOffset, TimeSpan? refreshInterval, int maxBufferSize, string writeJournalPluginId)
        {
            return refreshInterval.HasValue
                ? Actor.Props.Create(() => new LiveEventsByTagPublisher(tag, fromOffset, toOffset, refreshInterval.Value, maxBufferSize, writeJournalPluginId))
                : Actor.Props.Create(() => new CurrentEventsByTagPublisher(tag, fromOffset, toOffset, maxBufferSize, writeJournalPluginId));
        }
    }
}