using Akka.Actor;

namespace Akka.Persistence.KafkaRocks.Query
{
    internal sealed class CurrentEventsByTagPublisher : AbstractEventsByTagPublisher
    {
        public CurrentEventsByTagPublisher(string tag, long fromOffset, long toOffset, int maxBufferSize, string writeJournalPluginId)
            : base(tag, fromOffset, maxBufferSize, writeJournalPluginId)
        {
            ToOffset = toOffset;
        }

        protected override void ReceiveInitialRequest()
        {
            Replay();
        }

        protected override void ReceiveIdleRequest()
        {
            Buffer.DeliverBuffer(TotalDemand);
            if (Buffer.IsEmpty && CurrentOffset >= ToOffset)
                OnCompleteThenStop();
            else
                Self.Tell(EventsByTagPublisher.Continue.Instance);
        }

        protected override void ReceiveRecoverySuccess(long highestSequenceNr)
        {
            Buffer.DeliverBuffer(TotalDemand);
            if (highestSequenceNr < ToOffset)
                ToOffset = highestSequenceNr;

            if (Buffer.IsEmpty && (CurrentOffset >= ToOffset || CurrentOffset == FromOffset))
                OnCompleteThenStop();
            else
                Self.Tell(EventsByTagPublisher.Continue.Instance); // more to fetch

            Context.Become(Idle);
        }
    }
}