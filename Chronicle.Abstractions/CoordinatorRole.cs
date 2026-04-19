namespace Chronicle.Abstractions;

/// <summary>
/// Represents the role of an instance in a multi-instance deployment.
/// Used by <see cref="IInstanceCoordinator"/> to determine whether an instance
/// should actively read from the replication stream or stand by.
/// </summary>
public enum CoordinatorRole
{
    /// <summary>
    /// This instance is the active reader of the replication stream.
    /// Only one instance should be Active at any given time.
    /// </summary>
    Active,
    
    /// <summary>
    /// This instance is on standby and should not read from the replication stream.
    /// Standby instances poll periodically and become Active if the current Active instance fails.
    /// </summary>
    Standby
}