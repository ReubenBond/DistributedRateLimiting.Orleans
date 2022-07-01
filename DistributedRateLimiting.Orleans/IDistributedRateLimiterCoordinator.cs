using Orleans;

namespace DistributedRateLimiting.Orleans;

internal interface IDistributedRateLimiterCoordinator : IGrainWithStringKey
{
    ValueTask<int> TryAcquire(IRateLimiterClient client, int sequenceNumber, int permitCount);
    ValueTask Release(IRateLimiterClient client, int sequenceNumber, int permitCount);
    ValueTask RefreshLeases(IRateLimiterClient client);
    ValueTask Unregister(IRateLimiterClient client);
}
