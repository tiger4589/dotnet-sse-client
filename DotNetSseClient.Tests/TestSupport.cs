using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace DotNetSseClient.Tests;

internal sealed record TestMessage(string Name);

[JsonSourceGenerationOptions(JsonSerializerDefaults.Web)]
[JsonSerializable(typeof(TestMessage))]
internal sealed partial class TestJsonContext : JsonSerializerContext;

internal static class TestSupport
{
    public static readonly Uri BaseAddress = new("https://example.test/");

    /// <summary>Reconnect immediately and deterministically so tests stay fast.</summary>
    public static SseReconnectOptions FastReconnect() => new()
    {
        InitialDelay = TimeSpan.Zero,
        MinimumDelay = TimeSpan.Zero,
        MaxDelay = TimeSpan.Zero,
        JitterFactor = 0
    };

    public static ApiClient<TestMessage> CreateApiClient(
        HttpMessageHandler handler,
        Func<HttpRequestMessage, CancellationToken, ValueTask>? configureRequest = null,
        Func<HttpRequestMessage, CancellationToken, ValueTask<bool>>? onUnauthorized = null,
        SseReconnectOptions? reconnect = null,
        IReadOnlySet<string>? eventTypes = null,
        Func<string, TestMessage?>? deserialize = null)
    {
        return new ApiClient<TestMessage>(
            () => new HttpClient(handler, disposeHandler: false) { BaseAddress = BaseAddress },
            deserialize ?? (payload => JsonSerializer.Deserialize(payload, TestJsonContext.Default.TestMessage)),
            new SseConnectionSettings
            {
                ConfigureRequest = configureRequest,
                OnUnauthorized = onUnauthorized,
                Reconnect = reconnect ?? FastReconnect(),
                EventTypes = eventTypes
            });
    }

    public static HttpResponseMessage EventStream(string content)
    {
        var response = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(content + "\n", Encoding.UTF8)
        };
        response.Content.Headers.ContentType = new MediaTypeHeaderValue("text/event-stream");
        return response;
    }

    public static HttpResponseMessage Status(HttpStatusCode statusCode) => new(statusCode);

    public static Task<T> WithTimeout<T>(this Task<T> task) => task.WaitAsync(TimeSpan.FromSeconds(10));

    public static Task WithTimeout(this Task task) => task.WaitAsync(TimeSpan.FromSeconds(10));
}

internal sealed record RequestSnapshot(
    string? RequestUri,
    string? Authorization,
    string? LastEventId,
    string? Accept,
    string? ApiKey)
{
    public static RequestSnapshot From(HttpRequestMessage request)
    {
        return new RequestSnapshot(
            request.RequestUri?.ToString(),
            request.Headers.Authorization?.ToString(),
            request.Headers.TryGetValues("Last-Event-ID", out var eventIds) ? eventIds.Single() : null,
            request.Headers.Accept.SingleOrDefault()?.MediaType,
            request.Headers.TryGetValues("X-Api-Key", out var apiKeys) ? apiKeys.Single() : null);
    }
}

/// <summary>Answers each request with the next scripted response and records what was sent.</summary>
internal sealed class ScriptedHandler(
    Func<HttpRequestMessage, int, CancellationToken, Task<HttpResponseMessage>> send) : HttpMessageHandler
{
    private int _attempts;

    public ScriptedHandler(params Func<HttpResponseMessage>[] responses)
        : this((_, attempt, _) => Task.FromResult(
            attempt <= responses.Length
                ? responses[attempt - 1]()
                : new HttpResponseMessage(HttpStatusCode.NotFound)))
    {
    }

    public int Attempts => Volatile.Read(ref _attempts);

    public List<RequestSnapshot> Requests { get; } = [];

    protected override Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        var attempt = Interlocked.Increment(ref _attempts);
        lock (Requests)
        {
            Requests.Add(RequestSnapshot.From(request));
        }

        return send(request, attempt, cancellationToken);
    }
}

/// <summary>Returns a 200 event stream whose body never ends until the request is cancelled.</summary>
internal sealed class BlockingHandler : HttpMessageHandler
{
    public TaskCompletionSource RequestStarted { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public TaskCompletionSource RequestCancelled { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    protected override Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        RequestStarted.TrySetResult();

        var response = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StreamContent(new BlockingStream(RequestCancelled))
        };
        response.Content.Headers.ContentType = new MediaTypeHeaderValue("text/event-stream");
        return Task.FromResult(response);
    }

    private sealed class BlockingStream(TaskCompletionSource requestCancelled) : Stream
    {
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
                return 0;
            }
            catch (OperationCanceledException)
            {
                requestCancelled.TrySetResult();
                throw;
            }
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
