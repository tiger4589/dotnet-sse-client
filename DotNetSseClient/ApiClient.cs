using System.Net;
using System.Net.Http.Headers;
using System.Net.ServerSentEvents;
using System.Text.Json;

namespace DotNetSseClient;

internal sealed class ApiClient<T>(
    HttpClient httpClient,
    Func<HttpRequestMessage, CancellationToken, ValueTask>? configureRequest = null,
    Func<HttpRequestMessage, CancellationToken, ValueTask<bool>>? onUnauthorized = null) : IDisposable
{
    private readonly TimeSpan _defaultReconnectDelay = TimeSpan.FromSeconds(3);
    private readonly JsonSerializerOptions _serializerOptions = new(JsonSerializerDefaults.Web);

    public async Task StreamAsync(
        string endpoint,
        Action<T> onMessage,
        Action<Exception> onError,
        Action onDisconnected,
        CancellationToken cancellationToken)
    {
        var reconnectDelay = _defaultReconnectDelay;
        string? lastEventId = null;

        while (!cancellationToken.IsCancellationRequested)
        {
            var wasConnected = false;
            var hasRefreshedTokenOnCurrentConnectionAttempt = false;

            try
            {
                while (true)
                {
                    using var request = new HttpRequestMessage(HttpMethod.Get, endpoint);
                    request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/event-stream"));

                    if (!string.IsNullOrEmpty(lastEventId))
                    {
                        request.Headers.TryAddWithoutValidation("Last-Event-ID", lastEventId);
                    }

                    if (configureRequest is not null)
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
                            onError(new InvalidOperationException("Configuring the SSE request failed.", exception));
                            return;
                        }
                    }

                    using var response = await httpClient.SendAsync(
                        request,
                        HttpCompletionOption.ResponseHeadersRead,
                        cancellationToken).ConfigureAwait(false);

                    if (response.StatusCode == HttpStatusCode.Unauthorized
                        && onUnauthorized is not null
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
                            onError(new InvalidOperationException(
                                "Handling an SSE unauthorized response failed.",
                                exception));
                            return;
                        }

                        if (!shouldRetry)
                        {
                            onError(new HttpRequestException(
                                $"The SSE endpoint returned HTTP {(int)response.StatusCode} ({response.ReasonPhrase}).",
                                null,
                                response.StatusCode));
                            return;
                        }

                        hasRefreshedTokenOnCurrentConnectionAttempt = true;
                        continue;
                    }

                    if (!response.IsSuccessStatusCode)
                    {
                        var exception = new HttpRequestException(
                            $"The SSE endpoint returned HTTP {(int)response.StatusCode} ({response.ReasonPhrase}).",
                            null,
                            response.StatusCode);
                        onError(exception);

                        if (!IsRetryable(response.StatusCode))
                        {
                            return;
                        }

                        break;
                    }

                    if (!string.Equals(
                            response.Content.Headers.ContentType?.MediaType,
                            "text/event-stream",
                            StringComparison.OrdinalIgnoreCase))
                    {
                        onError(new InvalidOperationException(
                            "The SSE endpoint response must have a text/event-stream content type."));
                        return;
                    }

                    wasConnected = true;
                    await using var stream = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);

                    await ReadEventsAsync(
                        stream,
                        onMessage,
                        onError,
                        value => lastEventId = value,
                        value => reconnectDelay = value,
                        cancellationToken).ConfigureAwait(false);

                    onDisconnected();
                    break;
                }

            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception exception) when (exception is HttpRequestException or IOException)
            {
                onError(exception);
                if (wasConnected)
                {
                    onDisconnected();
                }
            }

            await DelayBeforeReconnectAsync(reconnectDelay, cancellationToken).ConfigureAwait(false);
        }
    }

    public void Dispose()
    {
        httpClient.Dispose();
    }

    private async Task ReadEventsAsync(
        Stream stream,
        Action<T> onMessage,
        Action<Exception> onError,
        Action<string> setLastEventId,
        Action<TimeSpan> setReconnectDelay,
        CancellationToken cancellationToken)
    {
        var parser = SseParser.Create(stream);

        await foreach (var item in parser.EnumerateAsync(cancellationToken).ConfigureAwait(false))
        {
            if (!string.IsNullOrEmpty(item.EventId) && !item.EventId.Contains('\0'))
            {
                setLastEventId(item.EventId);
            }

            if (item.ReconnectionInterval is { } reconnectInterval)
            {
                setReconnectDelay(reconnectInterval);
            }

            if (!string.IsNullOrEmpty(item.Data))
            {
                DispatchMessage(item.Data, onMessage, onError);
            }
        }
    }

    private void DispatchMessage(
        string payload,
        Action<T> onMessage,
        Action<Exception> onError)
    {
        try
        {
            var message = JsonSerializer.Deserialize<T>(payload, _serializerOptions);
            if (message is null)
            {
                throw new JsonException("The SSE data payload deserialized to null.");
            }

            onMessage(message);
        }
        catch (JsonException exception)
        {
            onError(exception);
        }
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