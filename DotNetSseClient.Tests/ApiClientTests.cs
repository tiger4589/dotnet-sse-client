using System.Net;
using System.Net.Http.Headers;
using System.Text;

namespace DotNetSseClient.Tests;

public sealed class ApiClientTests
{
    [Fact]
    public async Task StreamAsync_ParsesFramesAndRetriesAfterUnauthorizedResponse()
    {
        var requests = new List<RequestSnapshot>();
        var handler = new DelegateHandler((request, attempt, _) =>
        {
            requests.Add(RequestSnapshot.From(request));

            return Task.FromResult(attempt == 1
                ? EventStream(
                    """
                    : keep-alive
                    retry: 0
                    id: event-42
                    data: {"name":
                    data: "first"}

                    """)
                : new HttpResponseMessage(HttpStatusCode.Unauthorized));
        });
        using var httpClient = new HttpClient(handler);
        httpClient.BaseAddress = new Uri("https://example.test/");
        var issuedTokenCount = 0;
        var unauthorizedHandlerCalls = 0;
        using var apiClient = new ApiClient<TestMessage>(
            httpClient,
            (request, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                request.Headers.Authorization =
                    new AuthenticationHeaderValue("Bearer", $"token-{++issuedTokenCount}");
                return ValueTask.CompletedTask;
            },
            (_, cancellationToken) =>
            {
                cancellationToken.ThrowIfCancellationRequested();
                unauthorizedHandlerCalls++;
                return ValueTask.FromResult(true);
            });
        var messages = new List<TestMessage>();
        var errors = new List<Exception>();
        var disconnects = 0;

        await apiClient.StreamAsync(
            "events",
            messages.Add,
            errors.Add,
            () => disconnects++,
            CancellationToken.None);

        var message = Assert.Single(messages);
        Assert.Equal("first", message.Name);
        Assert.Equal(1, disconnects);
        Assert.Equal(3, requests.Count);
        Assert.NotNull(requests[0].Authorization);
        Assert.NotNull(requests[1].Authorization);
        Assert.NotNull(requests[2].Authorization);
        Assert.Equal(3, issuedTokenCount);
        Assert.Equal(1, unauthorizedHandlerCalls);
        Assert.Null(requests[0].LastEventId);
        Assert.Equal("event-42", requests[1].LastEventId);
        Assert.Equal("event-42", requests[2].LastEventId);
        Assert.Equal("text/event-stream", requests[0].Accept);
        Assert.IsType<HttpRequestException>(Assert.Single(errors));
    }

    [Fact]
    public async Task StreamAsync_ReportsInvalidJsonAndContinuesWithTheNextFrame()
    {
        var handler = new DelegateHandler((_, attempt, _) => Task.FromResult(
            attempt == 1
                ? EventStream(
                    """
                    data: not-json

                    data: {"name":"valid"}

                    """)
                : new HttpResponseMessage(HttpStatusCode.NotFound)));
        using var apiClient = new ApiClient<TestMessage>(
            new HttpClient(handler) { BaseAddress = new Uri("https://example.test/") });
        var messages = new List<TestMessage>();
        var errors = new List<Exception>();

        await apiClient.StreamAsync(
            "events",
            messages.Add,
            errors.Add,
            () => { },
            CancellationToken.None);

        Assert.Equal("valid", Assert.Single(messages).Name);
        Assert.Collection(
            errors,
            error => Assert.IsType<System.Text.Json.JsonException>(error),
            error => Assert.IsType<HttpRequestException>(error));
    }

    [Fact]
    public async Task StreamAsync_DoesNotSendWhenRequestConfigurationFails()
    {
        var handler = new DelegateHandler((_, _, _) =>
            throw new InvalidOperationException("The request must not be sent."));
        using var apiClient = new ApiClient<TestMessage>(
            new HttpClient(handler) { BaseAddress = new Uri("https://example.test/") },
            (_, _) => throw new AuthenticationException());
        var errors = new List<Exception>();

        await apiClient.StreamAsync(
            "events",
            _ => { },
            errors.Add,
            () => { },
            CancellationToken.None);

        var error = Assert.IsType<InvalidOperationException>(Assert.Single(errors));
        Assert.IsType<AuthenticationException>(error.InnerException);
        Assert.Equal(0, handler.Attempts);
    }

    private static HttpResponseMessage EventStream(string content)
    {
        var response = new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent(content + "\n", Encoding.UTF8)
        };
        response.Content.Headers.ContentType = new MediaTypeHeaderValue("text/event-stream");
        return response;
    }

    private sealed record TestMessage(string Name);

    private sealed class AuthenticationException : Exception
    {
    }

    private sealed record RequestSnapshot(
        string? Authorization,
        string? LastEventId,
        string? Accept)
    {
        public static RequestSnapshot From(HttpRequestMessage request)
        {
            return new RequestSnapshot(
                request.Headers.Authorization?.ToString(),
                request.Headers.TryGetValues("Last-Event-ID", out var eventIds)
                    ? eventIds.Single()
                    : null,
                request.Headers.Accept.SingleOrDefault()?.MediaType);
        }
    }

    private sealed class DelegateHandler(
        Func<HttpRequestMessage, int, CancellationToken, Task<HttpResponseMessage>> send) : HttpMessageHandler
    {
        public int Attempts { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            Attempts++;
            return send(request, Attempts, cancellationToken);
        }
    }
}
