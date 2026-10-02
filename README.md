# dotnet-sse-client

A lightweight Server-Sent Events (SSE) client built on `HttpClient`, with keyed DI registration for .NET applications.

## Requirements

- .NET 10 (`net10.0`)

## Installation

```bash
dotnet add package DotNetSseClient
```

## Quick start

```csharp
using DotNetSseClient;
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();
services.AddSseClient<MyEvent>(
    key: "events",
    baseAddress: "https://api.example.com/")
    .WithBearerToken((serviceProvider, cancellationToken) =>
    {
        var tokenProvider = serviceProvider.GetRequiredService<[YourTokenProvider]>();
        return ValueTask.FromResult(tokenProvider.GetToken(cancellationToken));
    })
    .OnUnauthorized((serviceProvider, _, cancellationToken) =>
    {
        var tokenProvider = serviceProvider.GetRequiredService<[YourTokenProvider]>();
        tokenProvider.Refresh(cancellationToken);
        return ValueTask.FromResult(true); // retry once with refreshed token
    });

await using var provider = services.BuildServiceProvider();
var stream = provider.GetRequiredKeyedService<SseClient<MyEvent>>("events");

await stream.StartAsync(
    "sse/updates",
    onMessage: message => Console.WriteLine(message),
    onError: ex => Console.Error.WriteLine(ex),
    onDisconnected: () => Console.WriteLine("Disconnected."));
```

## Why keyed DI Registration?

In case you need to consume two different SSE streams in the same application using the same type, you can register two `SseClient<T>` instances with different keys and configurations.

## Behavior notes

- Endpoints passed to `StartAsync` can be relative or absolute.
- `WithBearerToken` and `WithApiKey` are applied on every connect/reconnect attempt.
- `onUnauthorized` is invoked on HTTP 401 and can opt into one immediate retry.
- The client validates `text/event-stream` responses.
- Automatic reconnect is supported, including `Last-Event-ID` continuation.
- JSON parse errors are raised through the `Error` event and stream processing continues.
