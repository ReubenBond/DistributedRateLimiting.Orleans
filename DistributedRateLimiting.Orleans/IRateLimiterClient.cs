#nullable enable

using Orleans;

namespace DistributedRateLimiting.Orleans;

internal interface IRateLimiterClient : IGrainObserver
{
    void OnPermitsAvailable(int availablePermits);
}
