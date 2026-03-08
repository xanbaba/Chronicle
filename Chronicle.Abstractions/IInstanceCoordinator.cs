namespace Chronicle.Abstractions;

public interface IInstanceCoordinator
{
    public Task<CoordinatorRole> NegotiateRoleAsync(CancellationToken cancellationToken);
    public Task<CoordinatorRole> RenewAsync(CancellationToken cancellationToken);
    public Task ReleaseAsync(CancellationToken cancellationToken);
}