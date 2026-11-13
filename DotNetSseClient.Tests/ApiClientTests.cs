using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using static DotNetSseClient.Tests.TestSupport;

namespace DotNetSseClient.Tests;

public sealed class ApiClientTests
{
    private readonly List<TestMessage> _messages = [];
    private readonly List<Exception> _errors = [];
    private int _disconnects;

    [Fact]
    public async Task StreamAsync_ParsesFramesReconnectsAndRetriesOnceAfterUnauthorized()
    {
        var handler = new ScriptedHandler(
            () => EventStream(
                """
                : keep-alive
                id: event-42
                data: {"name":
                data: "first"}

                """),
            () => Status(HttpStatusCode.Unauthorized),
            () => Status(HttpStatusCode.Unauthorized));
        var issuedTokenCount = 0;
        var unauthorizedHandlerCalls = 0;
        var apiClient = CreateApiClient(
            handler,
            configureRequest: (request, _) =>
            {
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", $"token-{++issuedTokenCount}");
                return ValueTask.CompletedTask;
            },
            onUnauthorized: (_, _) =>
            {
                unauthorizedHandlerCalls++;
                return ValueTask.FromResult(true);
            });

        var terminated = await Assert.ThrowsAsync<SseStreamTerminatedException>(() => StreamAsync(apiClient));

        Assert.Equal(HttpStatusCode.Unauthorized, terminated.StatusCode);
        Assert.Equal("first", Assert.Single(_messages).Name);
        Assert.Equal(1, _disconnects);
        Assert.Empty(_errors);
        Assert.Equal(3, handler.Requests.Count);
        Assert.Equal(["Bearer token-1", "Bearer token-2", "Bearer token-3"], handler.Requests.Select(r => r.Authorization));
        Assert.Equal(1, unauthorizedHandlerCalls);
        Assert.Equal([null, "event-42", "event-42"], handler.Requests.Select(r => r.LastEventId));
        Assert.Equal("text/event-stream", handler.Requests[0].Accept);
    }

    [Fact]
    public async Task StreamAsync_ReportsInvalidJsonAndContinuesWithTheNextFrame()
    {
        var handler = new ScriptedHandler(() => EventStream(
            """
            data: not-json

            data: {"name":"valid"}

            """));

        await Assert.ThrowsAsync<SseStreamTerminatedException>(() => StreamAsync(CreateApiClient(handler)));

        Assert.Equal("valid", Assert.Single(_messages).Name);
        Assert.IsType<JsonException>(Assert.Single(_errors));
    }

    [Fact]
    public async Task StreamAsync_ReportsUnsupportedPayloadsAndContinues()
    {
        var handler = new ScriptedHandler(() => EventStream(
            """
            data: {"name":"unsupported"}

            data: {"name":"valid"}

            """));
        var apiClient = CreateApiClient(handler, deserialize: payload => payload.Contains("unsupported")
            ? throw new NotSupportedException()
            : new TestMessage("valid"));

        await Assert.ThrowsAsync<SseStreamTerminatedException>(() => StreamAsync(apiClient));

        Assert.Single(_messages);
        Assert.IsType<NotSupportedException>(Assert.Single(_errors));
    }

    [Fact]
    public async Task StreamAsync_DoesNotSendWhenRequestConfigurationFails()
    {
        var handler = new ScriptedHandler();
        var apiClient = CreateApiClient(handler, configureRequest: (_, _) => throw new FormatException());

        var terminated = await Assert.ThrowsAsync<SseStreamTerminatedException>(() => StreamAsync(apiClient));

        Assert.IsType<FormatException>(terminated.InnerException);
        Assert.Equal(0, handler.Attempts);
    }

    [Fact]
    public async Task StreamAsync_RetriesRetryableStatusCodes()
    {
        var handler = new ScriptedHandler(
            () => Status(HttpStatusCode.ServiceUnavailable),
            () => Status(HttpStatusCode.TooManyRequests),
            () => Status(HttpStatusCode.Forbidden));

        var terminated = await Assert.ThrowsAsync<SseStreamTerminatedException>(() => StreamAsync(CreateApiClient(handler)));

        Assert.Equal(HttpStatusCode.Forbidden, terminated.StatusCode);
        Assert.Equal(3, handler.Attempts);
        Assert.Equal(
            [HttpStatusCode.ServiceUnavailable, HttpStatusCode.TooManyRequests],
            _errors.Select(e => Assert.IsType<HttpRequestException>(e).StatusCode));
        Assert.Equal(0, _disconnects);
    }

    [Fact]
    public async Task StreamAsync_RetriesTimeoutsThatAreNotCallerCancellation()
    {
        var handler = new ScriptedHandler((_, attempt, _) => attempt == 1
            ? throw new TaskCanceledException("HttpClient timeout")
            : Task.FromResult(Status(HttpStatusCode.NotFound)));

        await Assert.ThrowsAsync<SseStreamTerminatedException>(() => StreamAsync(CreateApiClient(handler)));

        Assert.IsType<TaskCanceledException>(Assert.Single(_errors));
        Assert.Equal(2, handler.Attempts);
    }

    [Fact]
    public async Task StreamAsync_RejectsNonEventStreamContentType()
    {
        var handler = new ScriptedHandler(() => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent("{}", MediaTypeHeaderValue.Parse("application/json"))
        });

        var terminated = await Assert.ThrowsAsync<SseStreamTerminatedException>(() => StreamAsync(CreateApiClient(handler)));

        Assert.Contains("application/json", terminated.Message);
        Assert.Equal(1, handler.Attempts);
    }

    [Fact]
    public async Task StreamAsync_StopsWithoutErrorOnNoContent()
    {
        var handler = new ScriptedHandler(() => Status(HttpStatusCode.NoContent));

        await StreamAsync(CreateApiClient(handler)).WithTimeout();

        Assert.Equal(1, handler.Attempts);
        Assert.Empty(_errors);
    }

    [Fact]
    public async Task StreamAsync_ReturnsPromptlyWhenCancelledDuringReconnectDelay()
    {
        var firstRequest = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var handler = new ScriptedHandler((_, _, _) =>
        {
            firstRequest.TrySetResult();
            return Task.FromResult(Status(HttpStatusCode.ServiceUnavailable));
        });
        var reconnect = FastReconnect();
        reconnect.InitialDelay = reconnect.MinimumDelay = reconnect.MaxDelay = TimeSpan.FromHours(1);
        using var cancellation = new CancellationTokenSource();

        var streaming = StreamAsync(CreateApiClient(handler, reconnect: reconnect), cancellation.Token);
        await firstRequest.Task.WithTimeout();
        await cancellation.CancelAsync();

        await streaming.WithTimeout();
        Assert.Equal(1, handler.Attempts);
    }

    [Fact]
    public async Task StreamAsync_DeliversOnlyConfiguredEventTypes()
    {
        var handler = new ScriptedHandler(() => EventStream(
            """
            event: ping
            data: not-json

            data: {"name":"default"}

            event: update
            data: {"name":"update"}

            """));
        var apiClient = CreateApiClient(handler, eventTypes: new HashSet<string> { "message" });

        await Assert.ThrowsAsync<SseStreamTerminatedException>(() => StreamAsync(apiClient));

        Assert.Equal("default", Assert.Single(_messages).Name);
        Assert.Empty(_errors);
    }

    [Fact]
    public async Task StreamAsync_HonoursAnEmptyIdThatResetsLastEventId()
    {
        var handler = new ScriptedHandler(
            () => EventStream(
                """
                id: first
                data: {"name":"one"}

                id
                data: {"name":"two"}

                """),
            () => EventStream(
                """
                id: x
                data: {"name":"three"}

                """),
            () => EventStream(
                """
                data: {"name":"no id here"}

                """));

        await Assert.ThrowsAsync<SseStreamTerminatedException>(() => StreamAsync(CreateApiClient(handler)));

        // Reset after the first stream; "x" survives a stream whose events carry no id field.
        Assert.Equal([null, null, "x", "x"], handler.Requests.Select(r => r.LastEventId));
    }

    [Fact]
    public async Task StreamAsync_PassesCancellationTokenToAsyncHandler()
    {
        var handler = new ScriptedHandler(() => EventStream("data: {\"name\":\"a\"}\n"));
        var apiClient = CreateApiClient(handler);
        var tokenSeen = false;
        using var cancellation = new CancellationTokenSource();

        await Assert.ThrowsAsync<SseStreamTerminatedException>(() => apiClient.StreamAsync(
            "events",
            async (_, token) =>
            {
                await Task.Yield();
                tokenSeen = token.CanBeCanceled;
            },
            _errors.Add,
            () => { },
            cancellation.Token));

        Assert.True(tokenSeen);
    }

    private Task StreamAsync(ApiClient<TestMessage> apiClient, CancellationToken cancellationToken = default)
    {
        return apiClient.StreamAsync(
            "events",
            (message, _) =>
            {
                _messages.Add(message);
                return ValueTask.CompletedTask;
            },
            _errors.Add,
            () => _disconnects++,
            cancellationToken);
    }
}
