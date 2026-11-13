using System.Net;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using static DotNetSseClient.Tests.TestSupport;

namespace DotNetSseClient.Tests;

public sealed class DependencyInjectionTests
{
    private static readonly ServiceProviderOptions StrictOptions = new() { ValidateScopes = true, ValidateOnBuild = true };

    [Fact]
    public async Task AddSseClient_ResolvesOnlyTheRequestedKeyedService()
    {
        var services = new ServiceCollection();
        services.AddSseClient<TestMessage>("primary", "https://primary.example.test/");
        services.AddSseClient<TestMessage>("secondary", "https://secondary.example.test/");
        await using var provider = services.BuildServiceProvider(StrictOptions);

        var primary = provider.GetRequiredKeyedService<SseClient<TestMessage>>("primary");
        var secondary = provider.GetRequiredKeyedService<SseClient<TestMessage>>("secondary");

        Assert.NotSame(primary, secondary);
        Assert.Null(provider.GetService<SseClient<TestMessage>>());
        Assert.False(typeof(ApiClient<>).IsPublic);
        Assert.True(typeof(SseClient<>).IsPublic);
    }

    [Fact]
    public async Task AddSseClient_DefaultsToSingletonAndCanBeInjectedIntoSingletonsWithScopeValidation()
    {
        var services = new ServiceCollection();
        services.AddSseClient<TestMessage>("events", "https://example.test/");
        services.AddSingleton(sp => new Consumer(sp.GetRequiredKeyedService<SseClient<TestMessage>>("events")));
        await using var provider = services.BuildServiceProvider(StrictOptions);

        var consumer = provider.GetRequiredService<Consumer>();

        Assert.Same(consumer.Client, provider.GetRequiredKeyedService<SseClient<TestMessage>>("events"));
    }

    [Fact]
    public async Task AddSseClient_HonoursAnExplicitLifetime()
    {
        var services = new ServiceCollection();
        services.AddSseClient<TestMessage>("events", "https://example.test/", ServiceLifetime.Scoped);
        await using var provider = services.BuildServiceProvider(StrictOptions);

        await using var scope1 = provider.CreateAsyncScope();
        await using var scope2 = provider.CreateAsyncScope();
        var a = scope1.ServiceProvider.GetRequiredKeyedService<SseClient<TestMessage>>("events");

        Assert.Same(a, scope1.ServiceProvider.GetRequiredKeyedService<SseClient<TestMessage>>("events"));
        Assert.NotSame(a, scope2.ServiceProvider.GetRequiredKeyedService<SseClient<TestMessage>>("events"));
        Assert.Throws<InvalidOperationException>(() => provider.GetRequiredKeyedService<SseClient<TestMessage>>("events"));
    }

    [Fact]
    public void AddSseClient_RejectsADuplicateKey()
    {
        var services = new ServiceCollection();
        services.AddSseClient<TestMessage>("events", "https://example.test/");

        Assert.Throws<InvalidOperationException>(() =>
            services.AddSseClient<TestMessage>("events", "https://other.example.test/"));
        services.AddSseClient<string>("events", "https://example.test/");
    }

    [Theory]
    [InlineData("ftp://example.test/")]
    [InlineData("relative/path")]
    public void AddSseClient_RejectsNonHttpBaseAddresses(string baseAddress)
    {
        Assert.ThrowsAny<Exception>(() => new ServiceCollection().AddSseClient<TestMessage>("events", baseAddress));
    }

    [Fact]
    public async Task Builder_SendsBearerTokenAndApiKeyThroughTheConfiguredHttpClient()
    {
        var handler = new ScriptedHandler(() => Status(HttpStatusCode.NotFound));
        var services = new ServiceCollection();
        services.AddSingleton<TokenProvider>();
        services.AddSseClient<TestMessage>("secure", "https://secure.example.test/api/")
            .WithBearerToken((serviceProvider, cancellationToken) =>
                ValueTask.FromResult(serviceProvider.GetRequiredService<TokenProvider>().GetToken(cancellationToken)))
            .WithApiKey("X-Api-Key", "secret")
            .ConfigureHttpClient(builder => builder.ConfigurePrimaryHttpMessageHandler(() => handler));
        await using var provider = services.BuildServiceProvider(StrictOptions);
        var client = provider.GetRequiredKeyedService<SseClient<TestMessage>>("secure");

        await client.StartAsync("events", _ => { });
        await Assert.ThrowsAsync<SseStreamTerminatedException>(() => client.Completion).WithTimeout();

        var request = Assert.Single(handler.Requests);
        Assert.Equal("https://secure.example.test/api/events", request.RequestUri);
        Assert.Equal("Bearer token", request.Authorization);
        Assert.Equal("secret", request.ApiKey);
    }

    [Fact]
    public async Task Builder_OnUnauthorizedReceivesTheServiceProvider()
    {
        var handler = new ScriptedHandler(() => Status(HttpStatusCode.Unauthorized), () => Status(HttpStatusCode.NotFound));
        var services = new ServiceCollection();
        services.AddSingleton<TokenProvider>();
        services.AddSseClient<TestMessage>("secure", "https://secure.example.test/")
            .OnUnauthorized((serviceProvider, _, cancellationToken) =>
            {
                serviceProvider.GetRequiredService<TokenProvider>().Refresh(cancellationToken);
                return ValueTask.FromResult(true);
            })
            .ConfigureHttpClient(builder => builder.ConfigurePrimaryHttpMessageHandler(() => handler));
        await using var provider = services.BuildServiceProvider(StrictOptions);
        var client = provider.GetRequiredKeyedService<SseClient<TestMessage>>("secure");

        await client.StartAsync("events", _ => { });
        var terminated = await Assert.ThrowsAsync<SseStreamTerminatedException>(() => client.Completion).WithTimeout();

        Assert.Equal(HttpStatusCode.NotFound, terminated.StatusCode);
        Assert.Equal(1, provider.GetRequiredService<TokenProvider>().Refreshes);
    }

    [Fact]
    public async Task AddSseClient_WithJsonTypeInfo_DeserializesWithSourceGeneratedMetadata()
    {
        var received = await ReceiveSingleAsync(
            services => services.AddSseClient("events", "https://example.test/", TestJsonContext.Default.TestMessage),
            """{"name":"source-generated"}""");

        Assert.Equal("source-generated", received.Name);
    }

    [Fact]
    public async Task Builder_WithJsonSerializerOptions_ReplacesTheDefaults()
    {
        var options = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseUpper };

        var received = await ReceiveSingleAsync(
            services => services.AddSseClient<TestMessage>("events", "https://example.test/").WithJsonSerializerOptions(options),
            """{"NAME":"custom"}""");

        Assert.Equal("custom", received.Name);
    }

    [Fact]
    public async Task Builder_WithEventTypes_FiltersEvents()
    {
        var received = await ReceiveSingleAsync(
            services => services.AddSseClient<TestMessage>("events", "https://example.test/").WithEventTypes("update"),
            "{\"name\":\"ignored\"}\n\nevent: update\ndata: {\"name\":\"kept\"}");

        Assert.Equal("kept", received.Name);
    }

    [Fact]
    public void Builder_WithReconnectPolicy_ValidatesOptions()
    {
        var builder = new ServiceCollection().AddSseClient<TestMessage>("events", "https://example.test/");

        Assert.Throws<ArgumentOutOfRangeException>(() => builder.WithReconnectPolicy(o => o.JitterFactor = 2));
        builder.WithReconnectPolicy(o => o.MaxDelay = TimeSpan.FromMinutes(5));
    }

    [Fact]
    public async Task SynchronousProviderDisposal_DoesNotThrow()
    {
        var handler = new BlockingHandler();
        var services = new ServiceCollection();
        services.AddSseClient<TestMessage>("events", "https://example.test/")
            .ConfigureHttpClient(builder => builder.ConfigurePrimaryHttpMessageHandler(() => handler));
        var provider = services.BuildServiceProvider(StrictOptions);
        var client = provider.GetRequiredKeyedService<SseClient<TestMessage>>("events");
        await client.StartAsync("events", _ => { });
        await handler.RequestStarted.Task.WithTimeout();

        provider.Dispose();

        await handler.RequestCancelled.Task.WithTimeout();
    }

    private static async Task<TestMessage> ReceiveSingleAsync(
        Func<IServiceCollection, SseClientRegistrationBuilder<TestMessage>> register,
        string data)
    {
        var handler = new ScriptedHandler(() => EventStream($"data: {data}\n"));
        var services = new ServiceCollection();
        var fast = FastReconnect();
        register(services)
            .ConfigureHttpClient(builder => builder.ConfigurePrimaryHttpMessageHandler(() => handler))
            .WithReconnectPolicy(o =>
            {
                o.InitialDelay = fast.InitialDelay;
                o.MinimumDelay = fast.MinimumDelay;
                o.MaxDelay = fast.MaxDelay;
                o.JitterFactor = fast.JitterFactor;
            });

        await using var provider = services.BuildServiceProvider(StrictOptions);
        var client = provider.GetRequiredKeyedService<SseClient<TestMessage>>("events");
        var received = new List<TestMessage>();
        var errors = new List<Exception>();

        await client.StartAsync("events", received.Add, errors.Add);
        await Assert.ThrowsAsync<SseStreamTerminatedException>(() => client.Completion).WithTimeout();

        Assert.Single(errors);
        return Assert.Single(received);
    }

    private sealed record Consumer(SseClient<TestMessage> Client);

    private sealed class TokenProvider
    {
        public int Refreshes { get; private set; }

        public string GetToken(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return "token";
        }

        public void Refresh(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Refreshes++;
        }
    }
}
