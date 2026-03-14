namespace Chronicle.Abstractions;

public interface IReplicationSource : IAsyncDisposable
{
    public IAsyncEnumerable<RawChangeEvent> StreamAsync(ReplicationOptions options, CancellationToken cancellationToken);
    public Task ConfirmAsync(ReplicationOffset offset, CancellationToken cancellationToken);
}