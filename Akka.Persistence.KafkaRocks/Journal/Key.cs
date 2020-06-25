namespace Akka.Persistence.KafkaRocks.Journal
{
    public sealed class Key
    {
        public Key(int persistenceId, long sequenceNr, int mappingId)
        {
            PersistenceId = persistenceId;
            SequenceNr = sequenceNr;
            MappingId = mappingId;
        }

        public int PersistenceId { get; }

        public long SequenceNr { get; }

        public int MappingId { get; }
    }
}