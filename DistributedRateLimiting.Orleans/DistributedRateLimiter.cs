using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading.RateLimiting;

namespace DistributedRateLimiting.Orleans;

/// <summary>
/// <see cref="RateLimiter"/> implementation that helps manage concurrent access to a resource.
/// </summary>
public sealed class DistributedRateLimiter : RateLimiter, IRateLimiterClient
{
    private static readonly Lease SuccessfulLease = new Lease(true, null, 0);
    private static readonly Lease FailedLease = new Lease(false, null, 0);
    private static readonly Lease QueueLimitLease = new Lease(false, null, 0, "Queue limit reached");
    private static readonly double TickFrequency = (double)TimeSpan.TicksPerSecond / Stopwatch.Frequency;
    private readonly IGrainFactory _grainFactory;
    private readonly ILogger<DistributedRateLimiter> _logger;
    private readonly DistributedRateLimiterOptions _options;
    private readonly Queue<RequestRegistration> _queue = new Queue<RequestRegistration>();
    private readonly IDistributedRateLimiterCoordinator _coordinator;
    private readonly Task _processTask;
    private readonly SemaphoreSlim _processSignal = new(1);

    private int _localAvailablePermitCount;
    private int _outstandingRequestedPermitCount;
    private long? _idleSince = Stopwatch.GetTimestamp();
    private bool _disposed;

    // Use the queue as the lock field so we don't need to allocate another object for a lock and have another field in the object
    private object Lock => _queue;

    /// <inheritdoc />
    public override TimeSpan? IdleDuration => _idleSince is null ? null : new TimeSpan((long)((Stopwatch.GetTimestamp() - _idleSince) * TickFrequency));

    /// <summary>
    /// Initializes the <see cref="DistributedRateLimiter"/>.
    /// </summary>
    /// <param name="options">Options to specify the behavior of the <see cref="DistributedRateLimiter"/>.</param>
    public DistributedRateLimiter(
        ILogger<DistributedRateLimiter> logger,
        IOptions<DistributedRateLimiterOptions> options,
        IGrainFactory grainFactory)
    {
        _grainFactory = grainFactory;
        _logger = logger;
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _coordinator = grainFactory.GetGrain<IDistributedRateLimiterCoordinator>("default");
        _processTask = Task.Run(ProcessRequests);
    }

    /// <inheritdoc/>
    public override int GetAvailablePermits() => _localAvailablePermitCount;

    /// <inheritdoc/>
    protected override RateLimitLease AcquireCore(int permitCount)
    {
        // These amounts of resources can never be acquired
        if (permitCount > _options.GlobalPermitCount)
        {
            throw new ArgumentOutOfRangeException(nameof(permitCount), permitCount, "Permit limit exceeded");
        }

        ThrowIfDisposed();

        // Return SuccessfulLease or FailedLease to indicate limiter state
        if (permitCount == 0)
        {
            return _localAvailablePermitCount > 0 ? SuccessfulLease : FailedLease;
        }

        // Perf: Check SemaphoreSlim implementation instead of locking
        if (_localAvailablePermitCount >= permitCount)
        {
            lock (Lock)
            {
                if (TryLeaseUnsynchronized(permitCount, out RateLimitLease? lease))
                {
                    return lease;
                }
            }
        }

        return FailedLease;
    }

    /// <inheritdoc/>
    protected override ValueTask<RateLimitLease> WaitAsyncCore(int permitCount, CancellationToken cancellationToken = default)
    {
        // These amounts of resources can never be acquired
        if (permitCount > _options.GlobalPermitCount)
        {
            throw new ArgumentOutOfRangeException(nameof(permitCount), permitCount, "Permit limit exceeded");
        }

        // Return SuccessfulLease if requestedCount is 0 and resources are available
        if (permitCount == 0 && _localAvailablePermitCount > 0 && !_disposed)
        {
            return new ValueTask<RateLimitLease>(SuccessfulLease);
        }

        // Perf: Check SemaphoreSlim implementation instead of locking
        lock (Lock)
        {
            if (TryLeaseUnsynchronized(permitCount, out RateLimitLease? lease))
            {
                return new ValueTask<RateLimitLease>(lease);
            }

            // Avoid integer overflow by using subtraction instead of addition
            Debug.Assert(_options.QueueLimit >= _outstandingRequestedPermitCount);
            if (_options.QueueLimit - _outstandingRequestedPermitCount < permitCount)
            {
                return new ValueTask<RateLimitLease>(QueueLimitLease);
            }

            CancelQueueState tcs = new CancelQueueState(permitCount, this, cancellationToken);
            CancellationTokenRegistration ctr = default;
            if (cancellationToken.CanBeCanceled)
            {
                ctr = cancellationToken.Register(static obj =>
                {
                    ((CancelQueueState)obj!).TrySetCanceled();
                }, tcs);
            }

            RequestRegistration request = new RequestRegistration(permitCount, tcs, ctr);
            _queue.Enqueue(request);
            _outstandingRequestedPermitCount += permitCount;
            Debug.Assert(_outstandingRequestedPermitCount <= _options.QueueLimit);

            _processSignal.Release();
            return new ValueTask<RateLimitLease>(request.Tcs.Task);
        }
    }

    private bool TryLeaseUnsynchronized(int permitCount, [NotNullWhen(true)] out RateLimitLease? lease)
    {
        ThrowIfDisposed();

        // if permitCount is 0 we want to queue it if there are no available permits
        if (_localAvailablePermitCount >= permitCount && _localAvailablePermitCount != 0)
        {
            if (permitCount == 0)
            {
                // Edge case where the check before the lock showed 0 available permits but when we got the lock some permits were now available
                lease = SuccessfulLease;
                return true;
            }

            // If there are no items queued we can lease
            if (_outstandingRequestedPermitCount == 0)
            {
                _idleSince = null;
                _localAvailablePermitCount -= permitCount;
                Debug.Assert(_localAvailablePermitCount >= 0);
                lease = new Lease(true, this, permitCount);
                return true;
            }
        }

        lease = null;
        return false;
    }

    private void Release(int releaseCount)
    {
        lock (Lock)
        {
            if (_disposed)
            {
                return;
            }

            _localAvailablePermitCount += releaseCount;
            Debug.Assert(_localAvailablePermitCount <= _options.GlobalPermitCount);

            while (_queue.Count > 0)
            {
                RequestRegistration nextPendingRequest = _queue.Peek();

                if (_localAvailablePermitCount >= nextPendingRequest.Count)
                {
                    nextPendingRequest = _queue.Dequeue();

                    _localAvailablePermitCount -= nextPendingRequest.Count;
                    _outstandingRequestedPermitCount -= nextPendingRequest.Count;
                    Debug.Assert(_localAvailablePermitCount >= 0);

                    Lease lease = nextPendingRequest.Count == 0 ? SuccessfulLease : new Lease(true, this, nextPendingRequest.Count);
                    // Check if request was canceled
                    if (!nextPendingRequest.Tcs.TrySetResult(lease))
                    {
                        // Queued item was canceled so add count back
                        _localAvailablePermitCount += nextPendingRequest.Count;
                        // Updating queue count is handled by the cancellation code
                        _outstandingRequestedPermitCount += nextPendingRequest.Count;
                    }
                    nextPendingRequest.CancellationTokenRegistration.Dispose();
                    Debug.Assert(_outstandingRequestedPermitCount >= 0);
                }
                else
                {
                    break;
                }
            }

            if (_outstandingRequestedPermitCount == 0)
            {
                //Debug.Assert(_idleSince is null);
                //Debug.Assert(_outstandingRequestedPermitCount == 0);
                _idleSince = Stopwatch.GetTimestamp();
            }

            _processSignal.Release();
        }
    }

    private async Task ProcessRequests()
    {
        // RequestId allows for idempotency.
        // This is employed to prevent the remote service from process a given request twice.
        // For example, in the case of a retry after a communication failure.
        int requestId = 1;
        IRateLimiterClient? selfReference = null;
        var pendingToRelease = 0;
        var pendingToAcquire = 0;
        Stopwatch lastRefresh = Stopwatch.StartNew();
        while (true)
        {
            try
            {
                selfReference ??= await _grainFactory.CreateObjectReference<IRateLimiterClient>(this);

                // If there are no pending requests, wait until a signal arrives to indicate that there may be some acquire/release
                // work to process.
                if (pendingToRelease == 0 && pendingToAcquire == 0)
                {
                    if (lastRefresh.Elapsed > _options.ClientLeaseRefreshInterval)
                    {
                        await _coordinator.RefreshLeases(selfReference);
                        lastRefresh.Restart();
                    }
                    else
                    {
                        var periodUntilNextRefresh = _options.ClientLeaseRefreshInterval.TotalMilliseconds - lastRefresh.Elapsed.TotalMilliseconds;
                        await _processSignal.WaitAsync((int)periodUntilNextRefresh);
                    }
                }

                lock (Lock)
                {
                    // We first check that we don't have to retry a previous request.
                    if (pendingToAcquire == 0)
                    {
                        pendingToAcquire = Math.Clamp(0, _options.TargetPermitsPerClient - _localAvailablePermitCount, _options.TargetPermitsPerClient);

                        // If a large request comes in, make sure that we are servicing it.
                        if (_queue.TryPeek(out var request))
                        {
                            pendingToAcquire = Math.Max(pendingToAcquire, request.Count - _localAvailablePermitCount);
                        }
                    }
                }

                // If there is a pending request, try to fulfill it.
                if (pendingToAcquire > 0)
                {
                    var acquired = await _coordinator.TryAcquire(selfReference, requestId, pendingToAcquire);
                    ++requestId;
                    pendingToAcquire = 0;
                    Release(acquired);
                }

                lock (Lock)
                {
                    // We first check that we don't have to retry a previous request.
                    if (pendingToRelease == 0)
                    {
                        var surplus = _localAvailablePermitCount - _options.TargetPermitsPerClient;
                        if (surplus > 0)
                        {
                            pendingToRelease = surplus;
                            _localAvailablePermitCount -= pendingToRelease;
                        }
                    }
                }

                if (pendingToRelease > 0)
                {
                    await _coordinator.Release(selfReference, requestId, pendingToRelease);
                    ++requestId;
                    pendingToRelease = 0;
                }
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, "Exception satisfying RateLimiter request");
            }
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (!disposing)
        {
            return;
        }

        lock (Lock)
        {
            if (_disposed)
            {
                return;
            }
            _disposed = true;
            while (_queue.Count > 0)
            {
                RequestRegistration next = _queue.Dequeue();
                next.CancellationTokenRegistration.Dispose();
                next.Tcs.TrySetResult(FailedLease);
            }
        }
    }

    protected override ValueTask DisposeAsyncCore()
    {
        Dispose(true);

        return default;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(DistributedRateLimiter));
        }
    }

    public void OnPermitsAvailable(int availablePermits)
    {
        _processSignal.Release();
    }

    private sealed class Lease : RateLimitLease
    {
        private static readonly string[] s_allMetadataNames = new[] { MetadataName.ReasonPhrase.Name };

        private bool _disposed;
        private readonly DistributedRateLimiter? _limiter;
        private readonly int _count;
        private readonly string? _reason;

        public Lease(bool isAcquired, DistributedRateLimiter? limiter, int count, string? reason = null)
        {
            IsAcquired = isAcquired;
            _limiter = limiter;
            _count = count;
            _reason = reason;

            // No need to set the limiter if count is 0, Dispose will noop
            Debug.Assert(count == 0 ? limiter is null : true);
        }

        public override bool IsAcquired { get; }

        public override IEnumerable<string> MetadataNames => s_allMetadataNames;

        public override bool TryGetMetadata(string metadataName, out object? metadata)
        {
            if (_reason is not null && metadataName == MetadataName.ReasonPhrase.Name)
            {
                metadata = _reason;
                return true;
            }
            metadata = default;
            return false;
        }

        protected override void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;

            _limiter?.Release(_count);
        }
    }

    private readonly struct RequestRegistration
    {
        public RequestRegistration(int requestedCount, TaskCompletionSource<RateLimitLease> tcs,
            CancellationTokenRegistration cancellationTokenRegistration)
        {
            Count = requestedCount;
            // Perf: Use AsyncOperation<TResult> instead
            Tcs = tcs;
            CancellationTokenRegistration = cancellationTokenRegistration;
        }

        public int Count { get; }

        public TaskCompletionSource<RateLimitLease> Tcs { get; }

        public CancellationTokenRegistration CancellationTokenRegistration { get; }
    }

    private sealed class CancelQueueState : TaskCompletionSource<RateLimitLease>
    {
        private readonly int _permitCount;
        private readonly DistributedRateLimiter _limiter;
        private readonly CancellationToken _cancellationToken;

        public CancelQueueState(int permitCount, DistributedRateLimiter limiter, CancellationToken cancellationToken)
            : base(TaskCreationOptions.RunContinuationsAsynchronously)
        {
            _permitCount = permitCount;
            _limiter = limiter;
            _cancellationToken = cancellationToken;
        }

        public new bool TrySetCanceled()
        {
            if (TrySetCanceled(_cancellationToken))
            {
                lock (_limiter.Lock)
                {
                    _limiter._outstandingRequestedPermitCount -= _permitCount;
                }
                return true;
            }
            return false;
        }
    }
}
