using System.Net.Http.Headers;
using Microsoft.Extensions.DependencyInjection;

namespace DotNetSseClient;

public static class DependencyInjection
{
    public static SseClientRegistrationBuilder<T> AddSseClient<T>(
        this IServiceCollection services,
        string key,
        string baseAddress)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentException.ThrowIfNullOrWhiteSpace(baseAddress);

        var baseUri = CreateBaseUri(baseAddress);
        var options = new SseClientRegistrationOptions<T>();

        services.AddKeyedScoped<ApiClient<T>>(key, (serviceProvider, _) =>
            new ApiClient<T>(
                new HttpClient
                {
                    BaseAddress = baseUri,
                    Timeout = Timeout.InfiniteTimeSpan
                },
                (request, cancellationToken) =>
                    options.ConfigureRequest?.Invoke(serviceProvider, request, cancellationToken)
                    ?? ValueTask.CompletedTask,
                options.OnUnauthorized is null
                    ? null
                    : (request, cancellationToken) =>
                        options.OnUnauthorized(serviceProvider, request, cancellationToken)));

        services.AddKeyedScoped<SseClient<T>>(key, (serviceProvider, registeredKey) =>
            new SseClient<T>(
                serviceProvider.GetRequiredKeyedService<ApiClient<T>>(registeredKey)));

        return new SseClientRegistrationBuilder<T>(options);
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

public sealed class SseClientRegistrationBuilder<T>
{
    private readonly SseClientRegistrationOptions<T> _options;

    internal SseClientRegistrationBuilder(SseClientRegistrationOptions<T> options)
    {
        _options = options;
    }

    public SseClientRegistrationBuilder<T> OnUnauthorized(
        Func<IServiceProvider, HttpRequestMessage, CancellationToken, ValueTask<bool>> onUnauthorized)
    {
        ArgumentNullException.ThrowIfNull(onUnauthorized);
        _options.OnUnauthorized = onUnauthorized;
        return this;
    }

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
}