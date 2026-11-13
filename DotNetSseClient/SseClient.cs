namespace DotNetSseClient;

/// <summary>
/// Consumes a Server-Sent Events stream, deserializes each event's data as JSON <typeparamref name="T"/>,
/// and reconnects automatically after transient failures.
/// </summary>
/// <typeparam name="T">The type each event's <c>data</c> payload is deserialized into.</typeparam>
/// <remarks>Resolve instances with <c>GetRequiredKeyedService&lt;SseClient&lt;T&gt;&gt;(key)</c> after calling
/// <see cref="DependencyInjection.AddSseClient{T}(Microsoft.Extensions.DependencyInjection.IServiceCollection, string, string, Microsoft.Extensions.DependencyInjection.ServiceLifetime)"/>.</remarks>
public sealed class SseClient<T> : IAsyncDisposable, IDisposable
{
    private readonly ApiClient<T> _apiClient;
    private readonly Lock _stateLock = new();
    private CancellationTokenSource? _streamCancellation;
    private Task? _streamTask;
    private TaskCompletionSource? _completion;
    private Func<T, CancellationToken, ValueTask>? _onMessage;
    private Action<Exception>? _onError;
    private Action? _onDisconnected;
    private bool _disposed;

    internal SseClient(ApiClient<T> apiClient)
    {
        _apiClient = apiClient;
    }

    /// <summary>Whether a stream started by <c>StartAsync</c> is still running.</summary>
    public bool IsRunning
    {
        get
        {
            lock (_stateLock)
            {
                return _streamTask is { IsCompleted: false };
            }
        }
    }

    /// <summary>
    /// Completes when the current (or most recent) stream ends. It completes successfully when the stream is
    /// stopped, cancelled or ended by the server with 204 No Content, and faults with
    /// <see cref="SseStreamTerminatedException"/> when the stream gives up after a non-recoverable failure.
    /// Before the first start it is already completed.
    /// </summary>
    public Task Completion
    {
        get
        {
            lock (_stateLock)
            {
                return _completion?.Task ?? Task.CompletedTask;
            }
        }
    }

    /// <summary>
    /// Starts streaming in the background and returns once the stream has been scheduled.
    /// </summary>
    /// <param name="endpoint">A path relative to the registered base address, or an absolute URL.</param>
    /// <param name="onMessage">Called for each deserialized event. Exceptions are reported to <paramref name="onError"/>.</param>
    /// <param name="onError">Called for transient errors, invalid payloads, handler failures, and the final
    /// <see cref="SseStreamTerminatedException"/> if the stream gives up.</param>
    /// <param name="onDisconnected">Called when an established connection ends, before reconnecting.</param>
    /// <param name="cancellationToken">Stops the stream when cancelled.</param>
    /// <exception cref="InvalidOperationException">A stream is already running. Await <see cref="StopAsync"/> first.</exception>
    public Task StartAsync(
        string endpoint,
        Action<T> onMessage,
        Action<Exception>? onError = null,
        Action? onDisconnected = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(onMessage);

        return StartAsync(
            endpoint,
            (message, _) =>
            {
                onMessage(message);
                return ValueTask.CompletedTask;
            },
            onError,
            onDisconnected,
            cancellationToken);
    }

    /// <summary>
    /// Starts streaming in the background with an asynchronous message handler. The next event is not
    /// read until the handler's task completes.
    /// </summary>
    /// <inheritdoc cref="StartAsync(string, Action{T}, Action{Exception}?, Action?, CancellationToken)"/>
    public Task StartAsync(
        string endpoint,
        Func<T, CancellationToken, ValueTask> onMessage,
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
                throw new InvalidOperationException(
                    "The SSE stream is already running. Await StopAsync before starting it again.");
            }

            _streamCancellation?.Dispose();
            _streamCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            _completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _onMessage = onMessage;
            _onError = onError;
            _onDisconnected = onDisconnected;
            _streamTask = RunStreamAsync(endpoint, _completion, _streamCancellation.Token);
        }

        return Task.CompletedTask;
    }

    /// <summary>Stops the stream and waits for it to finish. Does nothing if no stream is running.</summary>
    /// <param name="cancellationToken">Stops waiting; the stream is still cancelled.</param>
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

        await streamCancellation.CancelAsync().ConfigureAwait(false);
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

    /// <summary>Stops the stream. The underlying HTTP resources are owned by <c>IHttpClientFactory</c>.</summary>
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
    }

    /// <summary>
    /// Requests the stream to stop without waiting for it. Prefer <see cref="DisposeAsync"/>; this exists so
    /// that synchronously disposed service providers can release the client.
    /// </summary>
    public void Dispose()
    {
        CancellationTokenSource? streamCancellation;

        lock (_stateLock)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            streamCancellation = _streamCancellation;
        }

        try
        {
            streamCancellation?.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // StopAsync completed concurrently.
        }
    }

    // Never faults: the outcome is published through `completion`, so StopAsync can always await this task.
    private async Task RunStreamAsync(
        string endpoint,
        TaskCompletionSource completion,
        CancellationToken cancellationToken)
    {
        await Task.Yield();

        try
        {
            await _apiClient.StreamAsync(
                endpoint,
                NotifyMessageReceivedAsync,
                NotifyError,
                NotifyDisconnected,
                cancellationToken).ConfigureAwait(false);

            completion.TrySetResult();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            completion.TrySetResult();
        }
        catch (Exception exception)
        {
            var terminated = exception as SseStreamTerminatedException
                ?? new SseStreamTerminatedException("The SSE stream failed unexpectedly.", exception);

            NotifyError(terminated);
            completion.TrySetException(terminated);

            // The error was already delivered to onError; don't raise UnobservedTaskException if nobody awaits Completion.
            _ = completion.Task.Exception;
        }
    }

    private async ValueTask NotifyMessageReceivedAsync(T message, CancellationToken cancellationToken)
    {
        if (_onMessage is null)
        {
            return;
        }

        try
        {
            await _onMessage(message, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
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
