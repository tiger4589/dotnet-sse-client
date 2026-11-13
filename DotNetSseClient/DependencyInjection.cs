using System.Diagnostics.CodeAnalysis;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;

namespace DotNetSseClient;

/// <summary>Registers keyed <see cref="SseClient{T}"/> instances.</summary>
public static class DependencyInjection
{
    private const string ReflectionJsonMessage =
        "Deserializes T with reflection-based System.Text.Json. For trimmed or Native AOT apps, use the overload that takes a JsonTypeInfo<T>.";

    /// <summary>
    /// Registers a keyed <see cref="SseClient{T}"/> that deserializes event data with reflection-based
    /// System.Text.Json (web defaults, unless overridden with
    /// <see cref="SseClientRegistrationBuilder{T}.WithJsonSerializerOptions"/>).
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="key">The service key used to resolve this client.</param>
    /// <param name="baseAddress">An absolute HTTP or HTTPS base address.</param>
    /// <param name="lifetime">The client's lifetime. Singleton (the default) suits long-lived streams consumed
    /// by hosted services.</param>
    [RequiresUnreferencedCode(ReflectionJsonMessage)]
    [RequiresDynamicCode(ReflectionJsonMessage)]
    public static SseClientRegistrationBuilder<T> AddSseClient<T>(
        this IServiceCollection services,
        string key,
        string baseAddress,
        ServiceLifetime lifetime = ServiceLifetime.Singleton)
    {
        return AddSseClientCore<T>(services, key, baseAddress, lifetime, jsonTypeInfo: null, CreateReflectionDeserializer<T>);
    }

    /// <summary>
    /// Registers a keyed <see cref="SseClient{T}"/> that deserializes event data with the given source-generated
    /// <paramref name="jsonTypeInfo"/>. Safe for trimmed and Native AOT apps.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="key">The service key used to resolve this client.</param>
    /// <param name="baseAddress">An absolute HTTP or HTTPS base address.</param>
    /// <param name="jsonTypeInfo">The metadata used to deserialize <typeparamref name="T"/>.</param>
    /// <param name="lifetime">The client's lifetime. Defaults to singleton.</param>
    public static SseClientRegistrationBuilder<T> AddSseClient<T>(
        this IServiceCollection services,
        string key,
        string baseAddress,
        JsonTypeInfo<T> jsonTypeInfo,
        ServiceLifetime lifetime = ServiceLifetime.Singleton)
    {
        ArgumentNullException.ThrowIfNull(jsonTypeInfo);
        return AddSseClientCore<T>(services, key, baseAddress, lifetime, jsonTypeInfo, fallbackDeserializer: null);
    }

    private static SseClientRegistrationBuilder<T> AddSseClientCore<T>(
        IServiceCollection services,
        string key,
        string baseAddress,
        ServiceLifetime lifetime,
        JsonTypeInfo<T>? jsonTypeInfo,
        Func<JsonSerializerOptions?, Func<string, T?>>? fallbackDeserializer)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentException.ThrowIfNullOrWhiteSpace(baseAddress);

        if (services.Any(descriptor =>
                descriptor.ServiceType == typeof(SseClient<T>)
                && Equals(descriptor.ServiceKey, key)))
        {
            throw new InvalidOperationException(
                $"An SseClient<{typeof(T).Name}> is already registered with the key '{key}'.");
        }

        var baseUri = CreateBaseUri(baseAddress);
        var options = new SseClientRegistrationOptions<T> { JsonTypeInfo = jsonTypeInfo };
        var httpClientName = $"DotNetSseClient:{typeof(T)}:{key}";

        var httpClientBuilder = services.AddHttpClient(httpClientName, httpClient =>
        {
            httpClient.BaseAddress = baseUri;
            httpClient.Timeout = Timeout.InfiniteTimeSpan;
        });

        services.Add(new ServiceDescriptor(
            typeof(SseClient<T>),
            key,
            (serviceProvider, _) =>
            {
                var httpClientFactory = serviceProvider.GetRequiredService<IHttpClientFactory>();
                var deserialize = CreateDeserializer(options, fallbackDeserializer);

                var settings = new SseConnectionSettings
                {
                    ConfigureRequest = options.ConfigureRequest is { } configureRequest
                        ? (request, cancellationToken) => configureRequest(serviceProvider, request, cancellationToken)
                        : null,
                    OnUnauthorized = options.OnUnauthorized is { } onUnauthorized
                        ? (request, cancellationToken) => onUnauthorized(serviceProvider, request, cancellationToken)
                        : null,
                    Reconnect = options.Reconnect.Clone(),
                    EventTypes = options.EventTypes
                };

                return new SseClient<T>(new ApiClient<T>(
                    () => httpClientFactory.CreateClient(httpClientName),
                    deserialize,
                    settings));
            },
            lifetime));

        return new SseClientRegistrationBuilder<T>(options, httpClientBuilder);
    }

    private static Func<string, T?> CreateDeserializer<T>(
        SseClientRegistrationOptions<T> options,
        Func<JsonSerializerOptions?, Func<string, T?>>? fallbackDeserializer)
    {
        if (options.JsonTypeInfo is { } jsonTypeInfo)
        {
            return payload => JsonSerializer.Deserialize(payload, jsonTypeInfo);
        }

        if (fallbackDeserializer is not null)
        {
            return fallbackDeserializer(options.SerializerOptions);
        }

        if (options.SerializerOptions is { } serializerOptions)
        {
            var typeInfo = (JsonTypeInfo<T>)serializerOptions.GetTypeInfo(typeof(T));
            return payload => JsonSerializer.Deserialize(payload, typeInfo);
        }

        throw new InvalidOperationException("No JSON metadata is configured for the SSE client.");
    }

    [RequiresUnreferencedCode(ReflectionJsonMessage)]
    [RequiresDynamicCode(ReflectionJsonMessage)]
    private static Func<string, T?> CreateReflectionDeserializer<T>(JsonSerializerOptions? serializerOptions)
    {
        var effectiveOptions = serializerOptions ?? new JsonSerializerOptions(JsonSerializerDefaults.Web);
        return payload => JsonSerializer.Deserialize<T>(payload, effectiveOptions);
    }

    private static Uri CreateBaseUri(string baseAddress)
    {
        var baseUri = new Uri(baseAddress, UriKind.Absolute);
        if (baseUri.Scheme is not ("http" or "https"))
        {
            throw new ArgumentException(
                "The SSE base address must use the HTTP or HTTPS scheme.",
                nameof(baseAddress));
        }

        return baseUri;
    }
}

/// <summary>Configures an <see cref="SseClient{T}"/> registration.</summary>
public sealed class SseClientRegistrationBuilder<T>
{
    private readonly SseClientRegistrationOptions<T> _options;
    private readonly IHttpClientBuilder _httpClientBuilder;

    internal SseClientRegistrationBuilder(SseClientRegistrationOptions<T> options, IHttpClientBuilder httpClientBuilder)
    {
        _options = options;
        _httpClientBuilder = httpClientBuilder;
    }

    /// <summary>
    /// Configures the underlying named <see cref="HttpClient"/>, for example to add delegating handlers,
    /// resilience policies, a proxy or client certificates.
    /// </summary>
    /// <remarks>The client's timeout is infinite by default; a shorter timeout would cut long-lived streams.</remarks>
    public SseClientRegistrationBuilder<T> ConfigureHttpClient(Action<IHttpClientBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        configure(_httpClientBuilder);
        return this;
    }

    /// <summary>
    /// Called on HTTP 401. Return <c>true</c> to retry the request once (for example after refreshing a token),
    /// or <c>false</c> to stop the stream.
    /// </summary>
    public SseClientRegistrationBuilder<T> OnUnauthorized(
        Func<IServiceProvider, HttpRequestMessage, CancellationToken, ValueTask<bool>> onUnauthorized)
    {
        ArgumentNullException.ThrowIfNull(onUnauthorized);
        _options.OnUnauthorized = onUnauthorized;
        return this;
    }

    /// <summary>Sends <c>Authorization: Bearer</c> with a token fetched on every connection attempt.</summary>
    public SseClientRegistrationBuilder<T> WithBearerToken(
        Func<IServiceProvider, CancellationToken, ValueTask<string>> tokenFactory)
    {
        ArgumentNullException.ThrowIfNull(tokenFactory);

        return ConfigureRequest(async (serviceProvider, request, cancellationToken) =>
        {
            var token = await tokenFactory(serviceProvider, cancellationToken).ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(token))
            {
                request.Headers.Authorization = null;
                return;
            }

            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
        });
    }

    /// <summary>Sends an API key header on every connection attempt.</summary>
    public SseClientRegistrationBuilder<T> WithApiKey(string headerName, string apiKey)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(headerName);
        ArgumentException.ThrowIfNullOrWhiteSpace(apiKey);

        return ConfigureRequest((_, request, _) =>
        {
            request.Headers.Remove(headerName);
            request.Headers.TryAddWithoutValidation(headerName, apiKey);
            return ValueTask.CompletedTask;
        });
    }

    /// <summary>Configures reconnect delays and backoff.</summary>
    public SseClientRegistrationBuilder<T> WithReconnectPolicy(Action<SseReconnectOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var reconnect = _options.Reconnect.Clone();
        configure(reconnect);
        reconnect.Validate();
        _options.Reconnect = reconnect;
        return this;
    }

    /// <summary>
    /// Delivers only events whose <c>event:</c> field matches one of <paramref name="eventTypes"/> (case-sensitive).
    /// Events without an <c>event:</c> field have the type <c>message</c>. By default every event is delivered.
    /// </summary>
    public SseClientRegistrationBuilder<T> WithEventTypes(params string[] eventTypes)
    {
        ArgumentNullException.ThrowIfNull(eventTypes);
        if (eventTypes.Length == 0)
        {
            throw new ArgumentException("At least one event type is required.", nameof(eventTypes));
        }

        foreach (var eventType in eventTypes)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(eventType, nameof(eventTypes));
        }

        _options.EventTypes = new HashSet<string>(eventTypes, StringComparer.Ordinal);
        return this;
    }

    /// <summary>
    /// Deserializes event data with these options instead of the defaults. Replaces any
    /// <see cref="JsonTypeInfo{T}"/> given at registration; for Native AOT, the options need a
    /// source-generated <see cref="JsonSerializerOptions.TypeInfoResolver"/> that covers <typeparamref name="T"/>.
    /// </summary>
    public SseClientRegistrationBuilder<T> WithJsonSerializerOptions(JsonSerializerOptions serializerOptions)
    {
        ArgumentNullException.ThrowIfNull(serializerOptions);
        _options.SerializerOptions = serializerOptions;
        _options.JsonTypeInfo = null;
        return this;
    }

    private SseClientRegistrationBuilder<T> ConfigureRequest(
        Func<IServiceProvider, HttpRequestMessage, CancellationToken, ValueTask> configureRequest)
    {
        if (_options.ConfigureRequest is null)
        {
            _options.ConfigureRequest = configureRequest;
            return this;
        }

        var previous = _options.ConfigureRequest;
        _options.ConfigureRequest = async (serviceProvider, request, cancellationToken) =>
        {
            await previous(serviceProvider, request, cancellationToken).ConfigureAwait(false);
            await configureRequest(serviceProvider, request, cancellationToken).ConfigureAwait(false);
        };
        return this;
    }
}

internal sealed class SseClientRegistrationOptions<T>
{
    public Func<IServiceProvider, HttpRequestMessage, CancellationToken, ValueTask>? ConfigureRequest { get; set; }

    public Func<IServiceProvider, HttpRequestMessage, CancellationToken, ValueTask<bool>>? OnUnauthorized { get; set; }

    public SseReconnectOptions Reconnect { get; set; } = new();

    public IReadOnlySet<string>? EventTypes { get; set; }

    public JsonTypeInfo<T>? JsonTypeInfo { get; set; }

    public JsonSerializerOptions? SerializerOptions { get; set; }
}
