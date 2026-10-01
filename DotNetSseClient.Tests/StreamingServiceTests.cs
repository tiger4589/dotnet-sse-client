using System.Net;
using System.Net.Http.Headers;
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
}
