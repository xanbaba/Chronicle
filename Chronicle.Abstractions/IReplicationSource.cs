namespace Chronicle.Abstractions;

public interface IReplicationSource
{
    public IAsyncEnumerable<RawChangeEvent> StreamAsync(ReplicationOptions options, CancellationToken cancellationToken);
}