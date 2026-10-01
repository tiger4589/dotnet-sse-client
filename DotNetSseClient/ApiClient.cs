using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;

namespace DotNetSseClient;

internal sealed class ApiClient<T>(
    HttpClient httpClient,
    Func<HttpRequestHeaders, CancellationToken, ValueTask>? configureHeaders = null) : IDisposable
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

            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Get, endpoint);
                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/event-stream"));

                if (!string.IsNullOrEmpty(lastEventId))
                {
                    request.Headers.TryAddWithoutValidation("Last-Event-ID", lastEventId);
                }

                if (configureHeaders is not null)
                {
                    try
                    {
                        await configureHeaders(request.Headers, cancellationToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        return;
                    }
                    catch (Exception exception)
                    {
                        onError(new InvalidOperationException("Configuring the SSE request headers failed.", exception));
                        return;
                    }
                }

                using var response = await httpClient.SendAsync(
                    request,
                    HttpCompletionOption.ResponseHeadersRead,
                    cancellationToken).ConfigureAwait(false);

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

                    await DelayBeforeReconnectAsync(reconnectDelay, cancellationToken).ConfigureAwait(false);
                    continue;
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
                using var reader = new StreamReader(stream);

                await ReadEventsAsync(
                    reader,
                    onMessage,
                    onError,
                    value => lastEventId = value,
                    value => reconnectDelay = value,
                    cancellationToken).ConfigureAwait(false);

                onDisconnected();
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
        StreamReader reader,
        Action<T> onMessage,
        Action<Exception> onError,
        Action<string> setLastEventId,
        Action<TimeSpan> setReconnectDelay,
        CancellationToken cancellationToken)
    {
        var dataLines = new List<string>();
        var isFirstLine = true;

        while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
        {
            if (isFirstLine)
            {
                line = line.TrimStart('\uFEFF');
                isFirstLine = false;
            }

            if (line.Length == 0)
            {
                DispatchMessage(dataLines, onMessage, onError);
                continue;
            }

            if (line[0] == ':')
            {
                continue;
            }

            var colonIndex = line.IndexOf(':');
            var field = colonIndex < 0 ? line : line[..colonIndex];
            var value = colonIndex < 0 ? string.Empty : line[(colonIndex + 1)..];
            if (value.StartsWith(' '))
            {
                value = value[1..];
            }

            switch (field)
            {
                case "data":
                    dataLines.Add(value);
                    break;

                case "id" when !value.Contains('\0'):
                    setLastEventId(value);
                    break;

                case "retry" when int.TryParse(
                    value,
                    NumberStyles.None,
                    CultureInfo.InvariantCulture,
                    out var retryMilliseconds):
                    setReconnectDelay(TimeSpan.FromMilliseconds(retryMilliseconds));
                    break;
            }
        }
    }

    private void DispatchMessage(
        List<string> dataLines,
        Action<T> onMessage,
        Action<Exception> onError)
    {
        if (dataLines.Count == 0)
        {
            return;
        }

        var payload = string.Join('\n', dataLines);
        dataLines.Clear();

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