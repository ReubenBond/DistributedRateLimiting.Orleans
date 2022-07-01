#nullable enable

namespace DistributedRateLimiting.Orleans;

/// <summary>
/// Options to specify the behavior of a <see cref="DistributedRateLimiter"/>.
/// </summary>
public sealed class DistributedRateLimiterOptions
{
    /// <summary>
    /// Maximum number of permits that can be leased by all rate limiters combined.
    /// </summary>
    public int GlobalPermitCount { get; set; }

    /// <summary>
    /// The number of permits to try to maintain leased on all clients.
    /// </summary>
    public int TargetPermitsPerClient { get; set; }

    /// <summary>
    /// Maximum number of permits that can be queued concurrently.
    /// </summary>
    public int QueueLimit { get; set; }

    /// <summary>
    /// The period of time after which to drop clients.
    /// </summary>
    public TimeSpan IdleClientTimeout { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    /// The period of time after which clients refresh their current leases.
    /// </summary>
    public TimeSpan ClientLeaseRefreshInterval { get; set; } = TimeSpan.FromSeconds(30);
}
