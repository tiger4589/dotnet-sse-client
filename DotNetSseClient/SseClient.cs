namespace DotNetSseClient;

public sealed class SseClient<T> : IAsyncDisposable
{
    private readonly ApiClient<T> _apiClient;
    private readonly Lock _stateLock = new();
    private CancellationTokenSource? _streamCancellation;
    private Task? _streamTask;
    private Action<T>? _onMessage;
    private Action<Exception>? _onError;
    private Action? _onDisconnected;
    private bool _disposed;

    internal SseClient(ApiClient<T> apiClient)
    {
        _apiClient = apiClient;
    }

    public Task StartAsync(
        string endpoint,
        Action<T> onMessage,
        Action<Exception>? onError = null,
        Action? onDisconnected = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint);
        ArgumentNullException.ThrowIfNull(onMessage);

        cancellationToken.ThrowIfCancellationRequested();

        lock (_stateLock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            if (_streamTask is { IsCompleted: false })
            {
                return Task.CompletedTask;
            }

            _streamCancellation?.Dispose();
            _streamCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _onMessage = onMessage;
            _onError = onError;
            _onDisconnected = onDisconnected;
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
        if (_onMessage is null)
        {
            return;
        }

        try
        {
            _onMessage(message);
        }
        catch (Exception exception)
        {
            NotifyError(new InvalidOperationException("An SSE message handler failed.", exception));
        }
    }

    private void NotifyError(Exception exception)
    {
        if (_onError is null)
        {
            return;
        }

        try
        {
            _onError(exception);
        }
        catch
        {
            // Subscriber failures must not terminate the stream or prevent cleanup.
        }
    }

    private void NotifyDisconnected()
    {
        if (_onDisconnected is null)
        {
            return;
        }

        try
        {
            _onDisconnected();
        }
        catch (Exception exception)
        {
            NotifyError(new InvalidOperationException("An SSE disconnection handler failed.", exception));
        }
    }
}
