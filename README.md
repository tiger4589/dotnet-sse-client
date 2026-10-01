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
    baseAddress: "https://api.example.com/",
    configureHeaders: (headers, _) =>
    {
        headers.Authorization = new("Bearer", "<token>");
        return ValueTask.CompletedTask;
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
- The client validates `text/event-stream` responses.
- Automatic reconnect is supported, including `Last-Event-ID` continuation.
- JSON parse errors are raised through the `Error` event and stream processing continues.

