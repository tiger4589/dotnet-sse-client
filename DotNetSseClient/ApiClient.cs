using System.Net;
using System.Net.Http.Headers;
using System.Net.ServerSentEvents;
using System.Text.Json;

namespace DotNetSseClient;

internal sealed class SseConnectionSettings
{
    public Func<HttpRequestMessage, CancellationToken, ValueTask>? ConfigureRequest { get; init; }

    public Func<HttpRequestMessage, CancellationToken, ValueTask<bool>>? OnUnauthorized { get; init; }

    public SseReconnectOptions Reconnect { get; init; } = new();

    /// <summary>Event types to deliver; <c>null</c> delivers every event type.</summary>
    public IReadOnlySet<string>? EventTypes { get; init; }
}

internal sealed class ApiClient<T>(
    Func<HttpClient> createHttpClient,
    Func<string, T?> deserialize,
    SseConnectionSettings? settings = null)
{
    private readonly SseConnectionSettings _settings = settings ?? new SseConnectionSettings();

    /// <summary>
    /// Streams events until <paramref name="cancellationToken"/> is cancelled (returns normally),
    /// the server answers 204 No Content (returns normally), or a non-recoverable failure occurs
    /// (throws <see cref="SseStreamTerminatedException"/>). Transient failures are reported through
    /// <paramref name="onError"/> and followed by a reconnect.
    /// </summary>
    public async Task StreamAsync(
        string endpoint,
        Func<T, CancellationToken, ValueTask> onMessage,
        Action<Exception> onError,
        Action onDisconnected,
        CancellationToken cancellationToken)
    {
        var reconnect = _settings.Reconnect;
        var baseDelay = reconnect.InitialDelay;
        var consecutiveFailures = 0;
        string? lastEventId = null;

        while (!cancellationToken.IsCancellationRequested)
        {
            var wasConnected = false;
            var receivedEvent = false;
            TimeSpan? retryAfter = null;
            var hasRefreshedTokenOnCurrentConnectionAttempt = false;

            try
            {
                using var httpClient = createHttpClient();

                while (true)
                {
                    using var request = new HttpRequestMessage(HttpMethod.Get, endpoint);
                    request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/event-stream"));
                    request.Headers.CacheControl = new CacheControlHeaderValue { NoCache = true };

                    if (!string.IsNullOrEmpty(lastEventId))
                    {
                        request.Headers.TryAddWithoutValidation("Last-Event-ID", lastEventId);
                    }

                    if (_settings.ConfigureRequest is { } configureRequest)
                    {
                        try
                        {
                            await configureRequest(request, cancellationToken).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                        {
                            return;
                        }
                        catch (Exception exception)
                        {
                            throw new SseStreamTerminatedException("Configuring the SSE request failed.", exception);
                        }
                    }

                    using var response = await httpClient.SendAsync(
                        request,
                        HttpCompletionOption.ResponseHeadersRead,
                        cancellationToken).ConfigureAwait(false);

                    if (response.StatusCode == HttpStatusCode.Unauthorized
                        && _settings.OnUnauthorized is { } onUnauthorized
                        && !hasRefreshedTokenOnCurrentConnectionAttempt)
                    {
                        bool shouldRetry;
                        try
                        {
                            shouldRetry = await onUnauthorized(request, cancellationToken).ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                        {
                            return;
                        }
                        catch (Exception exception)
                        {
                            throw new SseStreamTerminatedException(
                                "Handling an SSE unauthorized response failed.",
                                exception,
                                response.StatusCode);
                        }

                        if (!shouldRetry)
                        {
                            throw TerminalStatusError(response);
                        }

                        hasRefreshedTokenOnCurrentConnectionAttempt = true;
                        continue;
                    }

                    if (!response.IsSuccessStatusCode)
                    {
                        if (!IsRetryable(response.StatusCode))
                        {
                            throw TerminalStatusError(response);
                        }

                        retryAfter = GetRetryAfter(response);
                        onError(StatusError(response));
                        break;
                    }

                    // Per the SSE specification, 204 No Content tells the client to stop reconnecting.
                    if (response.StatusCode == HttpStatusCode.NoContent)
                    {
                        return;
                    }

                    if (!string.Equals(
                            response.Content.Headers.ContentType?.MediaType,
                            "text/event-stream",
                            StringComparison.OrdinalIgnoreCase))
                    {
                        throw new SseStreamTerminatedException(
                            $"The SSE endpoint response must have a text/event-stream content type, but was '{response.Content.Headers.ContentType?.MediaType ?? "(none)"}'.",
                            statusCode: response.StatusCode);
                    }

                    wasConnected = true;
                    await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
                    var parser = SseParser.Create(stream);

                    await foreach (var item in parser.EnumerateAsync(cancellationToken).ConfigureAwait(false))
                    {
                        if (!receivedEvent)
                        {
                            receivedEvent = true;
                            consecutiveFailures = 0;
                        }

                        // EventId is null when the event had no valid id field and "" when the server reset it.
                        if (item.EventId is not null)
                        {
                            lastEventId = item.EventId;
                        }

                        if (item.ReconnectionInterval is { } reconnectInterval)
                        {
                            baseDelay = reconnectInterval;
                        }

                        if (_settings.EventTypes is { } eventTypes && !eventTypes.Contains(item.EventType))
                        {
                            continue;
                        }

                        if (!string.IsNullOrEmpty(item.Data))
                        {
                            await DispatchMessageAsync(item.Data, onMessage, onError, cancellationToken).ConfigureAwait(false);
                        }
                    }

                    onDisconnected();
                    break;
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (SseStreamTerminatedException)
            {
                throw;
            }
            catch (Exception exception) when (exception is HttpRequestException or IOException or OperationCanceledException)
            {
                // OperationCanceledException here is not ours: typically an HttpClient timeout configured by the caller.
                onError(exception);
                if (wasConnected)
                {
                    onDisconnected();
                }
            }

            if (!receivedEvent)
            {
                consecutiveFailures++;
            }

            var delay = reconnect.ComputeDelay(baseDelay, consecutiveFailures, retryAfter);
            await DelayBeforeReconnectAsync(delay, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask DispatchMessageAsync(
        string payload,
        Func<T, CancellationToken, ValueTask> onMessage,
        Action<Exception> onError,
        CancellationToken cancellationToken)
    {
        T? message;
        try
        {
            message = deserialize(payload);
            if (message is null)
            {
                throw new JsonException("The SSE data payload deserialized to null.");
            }
        }
        catch (Exception exception) when (exception is JsonException or NotSupportedException)
        {
            onError(exception);
            return;
        }

        await onMessage(message, cancellationToken).ConfigureAwait(false);
    }

    private static HttpRequestException StatusError(HttpResponseMessage response)
    {
        return new HttpRequestException(
            $"The SSE endpoint returned HTTP {(int)response.StatusCode} ({response.ReasonPhrase}).",
            null,
            response.StatusCode);
    }

    private static SseStreamTerminatedException TerminalStatusError(HttpResponseMessage response)
    {
        var error = StatusError(response);
        return new SseStreamTerminatedException(error.Message, error, response.StatusCode);
    }

    private static TimeSpan? GetRetryAfter(HttpResponseMessage response)
    {
        var retryAfter = response.Headers.RetryAfter;
        if (retryAfter?.Delta is { } delta)
        {
            return delta;
        }

        if (retryAfter?.Date is { } date)
        {
            var delay = date - DateTimeOffset.UtcNow;
            return delay > TimeSpan.Zero ? delay : null;
        }

        return null;
    }

    private static bool IsRetryable(HttpStatusCode statusCode)
    {
        return statusCode is HttpStatusCode.RequestTimeout or HttpStatusCode.TooManyRequests
            || (int)statusCode >= 500;
    }

    private static async Task DelayBeforeReconnectAsync(
        TimeSpan reconnectDelay,
        CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(reconnectDelay, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }
}
