using System.Net;
using System.Net.Http.Headers;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;

namespace DotNetSseClient.Tests;

public sealed class StreamingServiceTests
{
    [Fact]
    public async Task StopAsync_CancelsAnActiveStream()
    {
        var handler = new BlockingHandler();
        var apiClient = new ApiClient<TestMessage>(
            new HttpClient(handler) { BaseAddress = new Uri("https://example.test/") });
        using var ownedApiClient = apiClient;
        await using var service = new StreamingService<TestMessage>(apiClient);

        await service.StartAsync("events");
        await handler.RequestStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.ThrowsAsync<InvalidOperationException>(() => service.StartAsync("other-events"));
        await service.StopAsync().WaitAsync(TimeSpan.FromSeconds(5));

        Assert.True(handler.RequestCancelled.Task.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task StartAsync_RejectsAnAbsoluteEndpoint()
    {
        var apiClient = new ApiClient<TestMessage>(
            new HttpClient { BaseAddress = new Uri("https://example.test/") });
        using var ownedApiClient = apiClient;
        await using var service = new StreamingService<TestMessage>(apiClient);

        await Assert.ThrowsAsync<ArgumentException>(
            () => service.StartAsync("https://other.example.test/events"));
    }

    [Fact]
    public async Task AddSseClient_ResolvesOnlyTheRequestedKeyedService()
    {
        var services = new ServiceCollection();
        services.AddSseClient<TestMessage>("primary", "https://primary.example.test/");
        services.AddSseClient<TestMessage>("secondary", "https://secondary.example.test/");
        await using var provider = services.BuildServiceProvider();

        var primary = provider.GetRequiredKeyedService<StreamingService<TestMessage>>("primary");
        var secondary = provider.GetRequiredKeyedService<StreamingService<TestMessage>>("secondary");

        Assert.NotSame(primary, secondary);
        Assert.Null(provider.GetService<StreamingService<TestMessage>>());
        Assert.False(typeof(ApiClient<>).IsPublic);
        Assert.True(typeof(StreamingService<>).IsPublic);
    }

    [Fact]
    public async Task AddSseClient_WithServiceProviderCallbacks_ResolvesSuccessfully()
    {
        var services = new ServiceCollection();
        services.AddSingleton<TokenProvider>();
        services.AddSseClient<TestMessage>(
            "secure",
            "https://secure.example.test/",
            configureRequest: (serviceProvider, request, cancellationToken) =>
            {
                var tokenProvider = serviceProvider.GetRequiredService<TokenProvider>();
                request.Headers.Authorization = new("Bearer", tokenProvider.GetToken(cancellationToken));
                return ValueTask.CompletedTask;
            },
            onUnauthorized: (serviceProvider, _, cancellationToken) =>
            {
                var tokenProvider = serviceProvider.GetRequiredService<TokenProvider>();
                tokenProvider.Refresh(cancellationToken);
                return ValueTask.FromResult(true);
            });
        await using var provider = services.BuildServiceProvider();

        var service = provider.GetRequiredKeyedService<StreamingService<TestMessage>>("secure");

        Assert.NotNull(service);
    }

    [Fact]
    public async Task MessageHandlerException_IsReportedThroughErrorEvent()
    {
        var apiClient = new ApiClient<TestMessage>(
            new HttpClient { BaseAddress = new Uri("https://example.test/") });
        using var ownedApiClient = apiClient;
        await using var service = new StreamingService<TestMessage>(apiClient);

        var errors = new List<Exception>();
        service.Error += errors.Add;
        service.MessageReceived += _ => throw new InvalidOperationException("Message callback failed.");

        InvokePrivate(service, "NotifyMessageReceived", new TestMessage("value"));

        var error = Assert.IsType<InvalidOperationException>(Assert.Single(errors));
        Assert.Equal("An SSE message handler failed.", error.Message);
        Assert.IsType<InvalidOperationException>(error.InnerException);
    }

    [Fact]
    public async Task DisconnectedHandlerException_IsReportedThroughErrorEvent()
    {
        var apiClient = new ApiClient<TestMessage>(
            new HttpClient { BaseAddress = new Uri("https://example.test/") });
        using var ownedApiClient = apiClient;
        await using var service = new StreamingService<TestMessage>(apiClient);

        var errors = new List<Exception>();
        service.Error += errors.Add;
        service.Disconnected += () => throw new InvalidOperationException("Disconnected callback failed.");

        InvokePrivate(service, "NotifyDisconnected");

        var error = Assert.IsType<InvalidOperationException>(Assert.Single(errors));
        Assert.Equal("An SSE disconnection handler failed.", error.Message);
        Assert.IsType<InvalidOperationException>(error.InnerException);
    }

    [Fact]
    public async Task ErrorHandlers_ContinueAfterSubscriberFailure()
    {
        var apiClient = new ApiClient<TestMessage>(
            new HttpClient { BaseAddress = new Uri("https://example.test/") });
        using var ownedApiClient = apiClient;
        await using var service = new StreamingService<TestMessage>(apiClient);

        var invoked = 0;
        service.Error += _ => throw new InvalidOperationException("First error handler failed.");
        service.Error += _ => invoked++;

        InvokePrivate(service, "NotifyError", new InvalidOperationException("Original error"));

        Assert.Equal(1, invoked);
    }

    private static void InvokePrivate(object instance, string methodName, params object?[]? parameters)
    {
        var method = instance.GetType().GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(method);
        method.Invoke(instance, parameters);
    }

    private sealed record TestMessage(string Name);

    private sealed class BlockingHandler : HttpMessageHandler
    {
        public TaskCompletionSource RequestStarted { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource RequestCancelled { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

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

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override async ValueTask<int> ReadAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken = default)
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

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }
    }

    private sealed class TokenProvider
    {
        public string GetToken(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return "token";
        }

        public void Refresh(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
        }
    }
}
