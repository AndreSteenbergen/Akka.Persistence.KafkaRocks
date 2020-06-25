using System.Threading.Tasks;
using Akka.Persistence.Snapshot;

namespace Akka.Persistence.KafkaRocks.Snapshot
{
    public class RocksDbSnapshotStore : SnapshotStore
    {
        protected override Task<SelectedSnapshot> LoadAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            throw new System.NotImplementedException();
        }

        protected override Task SaveAsync(SnapshotMetadata metadata, object snapshot)
        {
            throw new System.NotImplementedException();
        }

        protected override Task DeleteAsync(SnapshotMetadata metadata)
        {
            throw new System.NotImplementedException();
        }

        protected override Task DeleteAsync(string persistenceId, SnapshotSelectionCriteria criteria)
        {
            throw new System.NotImplementedException();
        }
    }
}