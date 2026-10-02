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
services.AddSingleton<TokenProvider>();
services.AddSseClient<MyEvent>(
    key: "events",
    baseAddress: "https://api.example.com/",
    configureRequest: (serviceProvider, request, cancellationToken) =>
    {
        var tokenProvider = serviceProvider.GetRequiredService<[YourTokenProvider]>();
        request.Headers.Authorization =
            new("Bearer", tokenProvider.GetToken(cancellationToken));
        return ValueTask.CompletedTask;
    },
    onUnauthorized: (serviceProvider, _, cancellationToken) =>
    {
        var tokenProvider = serviceProvider.GetRequiredService<[YourTokenProvider]>();
        tokenProvider.Refresh(cancellationToken);
        return ValueTask.FromResult(true); // retry once with refreshed token
    });

await using var provider = services.BuildServiceProvider();
var stream = provider.GetRequiredKeyedService<StreamingService<MyEvent>>("events");

stream.MessageReceived += message => Console.WriteLine(message);
stream.Error += ex => Console.Error.WriteLine(ex);
stream.Disconnected += () => Console.WriteLine("Disconnected.");

await stream.StartAsync("sse/updates");
```

## Behavior notes

- Endpoints passed to `StartAsync` must be relative to the configured base address.
- `configureRequest` is invoked for every connect/reconnect attempt.
- `onUnauthorized` is invoked on HTTP 401 and can opt into one immediate retry.
- The client validates `text/event-stream` responses.
- Automatic reconnect is supported, including `Last-Event-ID` continuation.
- JSON parse errors are raised through the `Error` event and stream processing continues.
