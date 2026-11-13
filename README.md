# dotnet-sse-client

A lightweight Server-Sent Events (SSE) client built on `HttpClient` and `IHttpClientFactory`, with keyed DI registration for .NET applications.

## Requirements

- .NET 10 (`net10.0`)
- .NET 11 (`net11.0`)

## Installation

```bash
dotnet add package DotNetSseClient
```

## Quick start

A long-lived stream usually lives in a hosted service. Clients are registered as singletons by default, so they can be injected directly:

```csharp
using DotNetSseClient;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton<ITokenProvider, MyTokenProvider>();
builder.Services.AddSseClient<MyEvent>(key: "events", baseAddress: "https://api.example.com/")
    .WithBearerToken((serviceProvider, cancellationToken) =>
        serviceProvider.GetRequiredService<ITokenProvider>().GetTokenAsync(cancellationToken))
    .OnUnauthorized(async (serviceProvider, _, cancellationToken) =>
    {
        await serviceProvider.GetRequiredService<ITokenProvider>().RefreshAsync(cancellationToken);
        return true; // retry once with the refreshed token
    });

builder.Services.AddHostedService<EventListener>();
await builder.Build().RunAsync();

sealed class EventListener([FromKeyedServices("events")] SseClient<MyEvent> stream, ILogger<EventListener> logger)
    : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await stream.StartAsync(
            "sse/updates",
            onMessage: async (message, cancellationToken) => await HandleAsync(message, cancellationToken),
            onError: ex => logger.LogWarning(ex, "SSE error"),
            onDisconnected: () => logger.LogInformation("Disconnected; reconnecting."),
            cancellationToken: stoppingToken);

        // Completes when the host stops; throws SseStreamTerminatedException if the stream gives up.
        await stream.Completion;
    }
}
```

`StartAsync` returns as soon as the stream is running in the background. Await `Completion` to wait for it to end, or call `StopAsync` to stop it.

## Why keyed DI registration?

If you consume two different SSE streams in the same application with the same payload type, register two `SseClient<T>` instances with different keys and configurations. Registering the same key twice for the same `T` throws.

## Configuration

| Builder method | Purpose |
| --- | --- |
| `WithBearerToken(factory)` | Sends `Authorization: Bearer …`, fetched on every connection attempt. |
| `WithApiKey(header, key)` | Sends an API key header on every connection attempt. |
| `OnUnauthorized(handler)` | Called on HTTP 401; return `true` to retry once. |
| `WithReconnectPolicy(o => …)` | Initial, minimum and maximum delay, backoff multiplier, jitter. |
| `WithEventTypes("update", …)` | Delivers only matching `event:` types (events without one are `message`). |
| `WithJsonSerializerOptions(options)` | Replaces the default `JsonSerializerDefaults.Web` options. |
| `ConfigureHttpClient(b => …)` | Access to the underlying `IHttpClientBuilder` for handlers, resilience, proxies, certificates. |

The lifetime can be changed with `AddSseClient<T>(key, baseAddress, ServiceLifetime.Scoped)`.

### Trimming and Native AOT

The default overload uses reflection-based System.Text.Json and is annotated accordingly. For trimmed or AOT apps, pass source-generated metadata:

```csharp
[JsonSerializable(typeof(MyEvent))]
partial class MyJsonContext : JsonSerializerContext;

services.AddSseClient("events", "https://api.example.com/", MyJsonContext.Default.MyEvent);
```

## Behavior notes

- Endpoints passed to `StartAsync` can be relative to the base address or absolute.
- Message handlers can be synchronous (`Action<T>`) or asynchronous (`Func<T, CancellationToken, ValueTask>`). An async handler is awaited before the next event is read.
- Calling `StartAsync` while a stream is running throws `InvalidOperationException`. Await `StopAsync` first. Check `IsRunning` to see whether a stream is active.
- The client validates `text/event-stream` responses and sends `Accept: text/event-stream` and `Cache-Control: no-cache`.
- **Reconnects** follow the server's `retry:` field (default 3 s), never faster than `MinimumDelay` (500 ms). They back off exponentially with jitter, up to `MaxDelay` (60 s), while attempts keep failing before an event is received. A `Retry-After` header on 429/503 is honoured.
- `Last-Event-ID` is resent on reconnect, and a server can reset it with an empty `id` field.
- Retryable failures (network errors, timeouts, 408, 429, 5xx) are reported through the `onError` callback and followed by a reconnect.
- Invalid JSON payloads and exceptions thrown by your handlers are reported through `onError`, and the stream continues.
- **Terminal failures** (other 4xx statuses, a refused 401, a wrong content type, a failing request callback) stop the stream. `onError` receives an `SseStreamTerminatedException` (with `StatusCode` when applicable), and `Completion` faults with the same exception.
- An HTTP 204 response stops the stream without an error, as the SSE specification requires.
