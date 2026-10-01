namespace DotNetSseClient;

public sealed class StreamingService<T> : IAsyncDisposable
{
    private readonly ApiClient<T> _apiClient;
    private readonly Lock _stateLock = new();
    private CancellationTokenSource? _streamCancellation;
    private Task? _streamTask;
    private bool _disposed;

    internal StreamingService(ApiClient<T> apiClient)
    {
        _apiClient = apiClient;
    }

    public event Action<T>? MessageReceived;

    public event Action<Exception>? Error;

    public event Action? Disconnected;

    public Task StartAsync(string endpoint, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint);
        if (Uri.TryCreate(endpoint, UriKind.Absolute, out _))
        {
            throw new ArgumentException(
                "The SSE endpoint must be relative to the configured base address.",
                nameof(endpoint));
        }

        cancellationToken.ThrowIfCancellationRequested();

        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_streamTask is { IsCompleted: false })
            {
                throw new InvalidOperationException("This streaming service is already running.");
            }

            _streamCancellation?.Dispose();
            _streamCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _streamTask = RunStreamAsync(endpoint, _streamCancellation.Token);
        }

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        Task? streamTask;
        CancellationTokenSource? streamCancellation;

        lock (_stateLock)
        {
            streamTask = _streamTask;
            streamCancellation = _streamCancellation;
        }

        if (streamTask is null || streamCancellation is null)
        {
            return;
        }

        await streamCancellation.CancelAsync();
        await streamTask.WaitAsync(cancellationToken).ConfigureAwait(false);

        lock (_stateLock)
        {
            if (ReferenceEquals(_streamTask, streamTask))
            {
                _streamTask = null;
                _streamCancellation = null;
                streamCancellation.Dispose();
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        lock (_stateLock)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
        }

        await StopAsync().ConfigureAwait(false);
        _apiClient.Dispose();
    }

    private async Task RunStreamAsync(string endpoint, CancellationToken cancellationToken)
    {
        await Task.Yield();

        try
        {
            await _apiClient.StreamAsync(
                endpoint,
                NotifyMessageReceived,
                NotifyError,
                NotifyDisconnected,
                cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception exception)
        {
            NotifyError(exception);
        }
    }

    private void NotifyMessageReceived(T message)
    {
        if (MessageReceived is not { } handlers)
        {
            return;
        }

        foreach (var @delegate in handlers.GetInvocationList())
        {
            var handler = (Action<T>)@delegate;
            try
            {
                handler(message);
            }
            catch (Exception exception)
            {
                NotifyError(new InvalidOperationException("An SSE message handler failed.", exception));
            }
        }
    }

    private void NotifyError(Exception exception)
    {
        if (Error is not { } handlers)
        {
            return;
        }

        foreach (var @delegate in handlers.GetInvocationList())
        {
            var handler = (Action<Exception>)@delegate;
            try
            {
                handler(exception);
            }
            catch
            {
                // Subscriber failures must not terminate the stream or prevent cleanup.
            }
        }
    }

    private void NotifyDisconnected()
    {
        if (Disconnected is not { } handlers)
        {
            return;
        }

        foreach (var @delegate in handlers.GetInvocationList())
        {
            var handler = (Action)@delegate;
            try
            {
                handler();
            }
            catch (Exception exception)
            {
                NotifyError(new InvalidOperationException("An SSE disconnection handler failed.", exception));
            }
        }
    }
}