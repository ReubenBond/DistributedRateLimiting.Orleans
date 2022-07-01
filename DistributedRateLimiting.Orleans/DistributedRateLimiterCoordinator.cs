#nullable enable

using Microsoft.Extensions.Options;
using Orleans;
using Orleans.Concurrency;
using System.Diagnostics;

namespace DistributedRateLimiting.Orleans;

[Reentrant]
internal class DistributedRateLimiterCoordinator : Grain, IDistributedRateLimiterCoordinator, IDisposable
{
    private readonly DistributedRateLimiterOptions _options;
    private readonly Dictionary<IRateLimiterClient, ClientState> _clients = new();
    private readonly Queue<IRateLimiterClient> _requests = new();
    private IDisposable? _clientPurgeTimer;
    private int _availablePermits;

    public DistributedRateLimiterCoordinator(IOptions<DistributedRateLimiterOptions> options)
    {
        _options = options.Value;
        _availablePermits = options.Value.GlobalPermitCount;
    }

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        _clientPurgeTimer = this.RegisterTimer(static state => ((DistributedRateLimiterCoordinator)state).OnPurgeClients(), this, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));
        return Task.CompletedTask;
    }

    public ValueTask<int> TryAcquire(IRateLimiterClient client, int sequenceNumber, int permitCount)
    {
        if (!_clients.TryGetValue(client, out var state))
        {
            state = _clients[client] = new ClientState();
        }

        state.LastSeen.Restart();
        var existingSequenceNumber = state.SequenceNumber;
        if (existingSequenceNumber >= sequenceNumber)
        {
            // Respond to duplicate requests with the result of the last request.
            // This allows for acquire requests to be safely retried.
            // The assumption/requirement here is that the client is awaiting those requests in sequence.
            return new(state.LastAcquiredPermitCount);
        }

        DropIdleClients();

        // Acquire additional permits
        int acquiredPermits;
        if (_availablePermits >= permitCount)
        {
            state.InUsePermitCount += permitCount;
            _availablePermits -= permitCount;

            if (state.HasPendingRequest)
            {
                state.HasPendingRequest = false;
            }

            acquiredPermits = permitCount;
        }
        else
        {
            // Enqueue the client so that its request can be serviced later.
            if (!state.HasPendingRequest)
            {
                state.PermitsToAcquire = permitCount;
                state.HasPendingRequest = true;
                _requests.Enqueue(client);
            }

            acquiredPermits = 0;
        }

        ServicePendingRequests();

        state.SequenceNumber = sequenceNumber;
        state.LastAcquiredPermitCount = acquiredPermits;
        return new(acquiredPermits);
    }

    public ValueTask Release(IRateLimiterClient client, int sequenceNumber, int permitCount)
    {
        if (!_clients.TryGetValue(client, out var state))
        {
            state = _clients[client] = new ClientState();
        }

        state.LastSeen.Restart();
        var existingSequenceNumber = state.SequenceNumber;
        if (existingSequenceNumber >= sequenceNumber)
        {
            // Ignore duplicate requests: the result has already been incorporated into this instance's state.
            return default;
        }

        DropIdleClients();

        state.InUsePermitCount -= permitCount;
        _availablePermits = Math.Min(_availablePermits + permitCount, _options.GlobalPermitCount);

        ServicePendingRequests();

        state.SequenceNumber = sequenceNumber;
        state.LastAcquiredPermitCount = 0;
        return default;
    }

    public ValueTask RefreshLeases(IRateLimiterClient client)
    {
        if (_clients.TryGetValue(client, out var state))
        {
            state.LastSeen.Restart();
        }

        return default;
    }

    public ValueTask Unregister(IRateLimiterClient client)
    {
        if (_clients.TryGetValue(client, out var state))
        {
            RemoveClient(client, state);
        }

        return default;
    }

    private void ServicePendingRequests()
    {
        while (_availablePermits > 0 && _requests.TryPeek(out var client))
        {
            if (!_clients.TryGetValue(client, out var state))
            {
                // The client must have been dropped
                _ = _requests.Dequeue();
                continue;
            }

            if (_availablePermits > state.PermitsToAcquire)
            {
                // Asynchronously signal the client to tell it that there are some permits available.
                client.OnPermitsAvailable(_availablePermits);
                state.HasPendingRequest = false;
                _requests.Dequeue();
            }
            else
            {
                break;
            }
        }
    }

    private Task OnPurgeClients()
    {
        DropIdleClients();
        ServicePendingRequests();
        return Task.CompletedTask;
    }

    private void DropIdleClients()
    {
        var dropped = default(List<KeyValuePair<IRateLimiterClient, ClientState>>);
        foreach (var (client, state) in _clients)
        {
            if (state.LastSeen.Elapsed > _options.IdleClientTimeout)
            {
                dropped ??= new();
                dropped.Add(new KeyValuePair<IRateLimiterClient, ClientState>(client, state));
            }
        }

        if (dropped is { Count: > 0 })
        {
            foreach (var (client, state) in dropped)
            {
                var removed = RemoveClient(client, state);
                Debug.Assert(removed);
            }
        }
    }

    private bool RemoveClient(IRateLimiterClient client, ClientState? state)
    {
        if (state is not null)
        {
            // Return all in-use permits to the pool.
            _availablePermits += state.InUsePermitCount;
        }

        return _clients.Remove(client);
    }

    public void Dispose()
    {
        _clientPurgeTimer?.Dispose();
    }

    private class ClientState
    {
        public ClientState()
        {
            LastSeen = Stopwatch.StartNew();
        }

        // Monotinically increasing sequence number for this client
        public int SequenceNumber { get; set; }

        // The permits which are currently in-use by this client
        public int InUsePermitCount { get; set; }

        // The unfullfilled permit count of the next request for the client
        public int PermitsToAcquire { get; set; }

        public bool HasPendingRequest { get; set; }

        // The number of permits which were acquired in the previous request.
        public int LastAcquiredPermitCount { get; set; }

        public Stopwatch LastSeen { get; }
    }
}
