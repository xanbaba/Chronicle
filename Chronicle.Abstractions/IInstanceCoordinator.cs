namespace Chronicle.Abstractions;

/// <summary>
/// Coordinates which instance in a multi-instance deployment is responsible for reading the replication stream.
/// Implementations determine the role of each instance — Active or Standby.
/// </summary>
/// <remarks>
/// <para>
/// A replication slot allows only one active connection at a time. This interface provides the mechanism
/// for multiple instances to coordinate who holds that single active connection.
/// </para>
/// <para>
/// Chronicle ships with two built-in implementations:
/// <list type="bullet">
///   <item><description>SingleInstanceCoordinator — always returns Active, no coordination needed.</description></item>
///   <item><description>LeaderElectionCoordinator — uses a distributed lock (e.g., Redis) to elect one Active instance.</description></item>
/// </list>
/// </para>
/// <para>
/// Third parties can implement this interface against any locking primitive (etcd, ZooKeeper, SQL Server, etc.)
/// by referencing only Chronicle.Abstractions.
/// </para>
/// </remarks>
public interface IInstanceCoordinator
{
    /// <summary>
    /// Called on startup to determine the instance's role. A non-blocking attempt to become Active.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the negotiation.</param>
    /// <returns>
    /// The assigned role — <see cref="CoordinatorRole.Active"/> if this instance should read the stream,
    /// or <see cref="CoordinatorRole.Standby"/> if it should wait.
    /// </returns>
    public Task<CoordinatorRole> NegotiateRoleAsync(CancellationToken cancellationToken);
    
    /// <summary>
    /// Called periodically by the Active instance as a heartbeat to maintain its role.
    /// Also allows the coordinator to revoke or reassign the role between heartbeats.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the renewal.</param>
    /// <returns>
    /// The current role — typically <see cref="CoordinatorRole.Active"/> if still holding the lock,
    /// or <see cref="CoordinatorRole.Standby"/> if the role was revoked.
    /// </returns>
    public Task<CoordinatorRole> RenewAsync(CancellationToken cancellationToken);
    
    /// <summary>
    /// Called on graceful shutdown to release the role immediately, enabling faster failover
    /// without waiting for the lock TTL to expire.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the release.</param>
    /// <returns>A task representing the asynchronous release operation.</returns>
    public Task ReleaseAsync(CancellationToken cancellationToken);
}