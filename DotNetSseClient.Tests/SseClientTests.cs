using System.Net;
using static DotNetSseClient.Tests.TestSupport;

namespace DotNetSseClient.Tests;

public sealed class SseClientTests
{
    [Fact]
    public async Task StartAsync_WhenAlreadyRunning_Throws()
    {
        var handler = new BlockingHandler();
        await using var client = new SseClient<TestMessage>(CreateApiClient(handler));

        await client.StartAsync("events", _ => { });
        await handler.RequestStarted.Task.WithTimeout();

        await Assert.ThrowsAsync<InvalidOperationException>(() => client.StartAsync("other-events", _ => { }));
        Assert.True(client.IsRunning);

        await client.StopAsync().WithTimeout();

        Assert.True(handler.RequestCancelled.Task.IsCompletedSuccessfully);
        Assert.False(client.IsRunning);
        await client.Completion.WithTimeout();
    }

    [Fact]
    public async Task StartAsync_CanRestartAfterStop()
    {
        var handler = new BlockingHandler();
        await using var client = new SseClient<TestMessage>(CreateApiClient(handler));

        await client.StartAsync("events", _ => { });
        await client.StopAsync().WithTimeout();
        await client.StartAsync("events", _ => { });

        Assert.True(client.IsRunning);
    }

    [Fact]
    public async Task StartAsync_AllowsAnAbsoluteEndpoint()
    {
        var handler = new ScriptedHandler();
        await using var client = new SseClient<TestMessage>(CreateApiClient(handler));

        await client.StartAsync("https://other.example.test/events", _ => { });
        await Assert.ThrowsAsync<SseStreamTerminatedException>(() => client.Completion).WithTimeout();

        Assert.Equal("https://other.example.test/events", Assert.Single(handler.Requests).RequestUri);
    }

    [Fact]
    public async Task Completion_FaultsAndOnErrorIsNotifiedWhenTheStreamGivesUp()
    {
        var handler = new ScriptedHandler(() => Status(HttpStatusCode.NotFound));
        await using var client = new SseClient<TestMessage>(CreateApiClient(handler));
        var errors = new List<Exception>();

        await client.StartAsync("events", _ => { }, errors.Add);
        var terminated = await Assert.ThrowsAsync<SseStreamTerminatedException>(() => client.Completion).WithTimeout();

        Assert.Equal(HttpStatusCode.NotFound, terminated.StatusCode);
        Assert.Same(terminated, Assert.Single(errors));
        Assert.False(client.IsRunning);
        await client.StopAsync().WithTimeout();
    }

    [Fact]
    public async Task MessageHandlerException_IsReportedAndStreamContinues()
    {
        var handler = new ScriptedHandler(() => EventStream(
            """
            data: {"name":"boom"}

            data: {"name":"ok"}

            """));
        await using var client = new SseClient<TestMessage>(CreateApiClient(handler));
        var received = new List<string>();
        var errors = new List<Exception>();

        await client.StartAsync(
            "events",
            message =>
            {
                received.Add(message.Name);
                if (message.Name == "boom")
                {
                    throw new FormatException("Message callback failed.");
                }
            },
            errors.Add);
        await Assert.ThrowsAsync<SseStreamTerminatedException>(() => client.Completion).WithTimeout();

        Assert.Equal(["boom", "ok"], received);
        var handlerError = Assert.IsType<InvalidOperationException>(errors[0]);
        Assert.Equal("An SSE message handler failed.", handlerError.Message);
        Assert.IsType<FormatException>(handlerError.InnerException);
        Assert.IsType<SseStreamTerminatedException>(errors[1]);
    }

    [Fact]
    public async Task AsyncMessageHandler_IsAwaitedBeforeTheNextEvent()
    {
        var handler = new ScriptedHandler(() => EventStream(
            """
            data: {"name":"one"}

            data: {"name":"two"}

            """));
        await using var client = new SseClient<TestMessage>(CreateApiClient(handler));
        var log = new List<string>();

        await client.StartAsync(
            "events",
            async (message, cancellationToken) =>
            {
                log.Add($"start {message.Name}");
                await Task.Delay(20, cancellationToken);
                log.Add($"end {message.Name}");
            });
        await Assert.ThrowsAsync<SseStreamTerminatedException>(() => client.Completion).WithTimeout();

        Assert.Equal(["start one", "end one", "start two", "end two"], log);
    }

    [Fact]
    public async Task DisconnectedHandlerException_IsReportedThroughOnErrorCallback()
    {
        var handler = new ScriptedHandler(() => EventStream("data: {\"name\":\"a\"}\n"));
        await using var client = new SseClient<TestMessage>(CreateApiClient(handler));
        var errors = new List<Exception>();

        await client.StartAsync(
            "events",
            _ => { },
            errors.Add,
            () => throw new FormatException("Disconnected callback failed."));
        await Assert.ThrowsAsync<SseStreamTerminatedException>(() => client.Completion).WithTimeout();

        var error = Assert.IsType<InvalidOperationException>(errors[0]);
        Assert.Equal("An SSE disconnection handler failed.", error.Message);
        Assert.IsType<FormatException>(error.InnerException);
    }

    [Fact]
    public async Task OnErrorFailure_DoesNotPreventCompletion()
    {
        var handler = new ScriptedHandler(() => Status(HttpStatusCode.NotFound));
        await using var client = new SseClient<TestMessage>(CreateApiClient(handler));

        await client.StartAsync("events", _ => { }, _ => throw new FormatException("Error callback failed."));

        await Assert.ThrowsAsync<SseStreamTerminatedException>(() => client.Completion).WithTimeout();
    }

    [Fact]
    public async Task Dispose_CancelsTheRunningStream()
    {
        var handler = new BlockingHandler();
        var client = new SseClient<TestMessage>(CreateApiClient(handler));

        await client.StartAsync("events", _ => { });
        await handler.RequestStarted.Task.WithTimeout();
        client.Dispose();

        await handler.RequestCancelled.Task.WithTimeout();
        await client.Completion.WithTimeout();
        await Assert.ThrowsAsync<ObjectDisposedException>(() => client.StartAsync("events", _ => { }));
    }
}
