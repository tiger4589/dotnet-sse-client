using Microsoft.Extensions.DependencyInjection;

namespace DotNetSseClient;

public static class DependencyInjection
{
    public static IServiceCollection AddSseClient<T>(
        this IServiceCollection services,
        string key,
        string baseAddress,
        Func<IServiceProvider, HttpRequestMessage, CancellationToken, ValueTask>? configureRequest = null,
        Func<IServiceProvider, HttpRequestMessage, CancellationToken, ValueTask<bool>>? onUnauthorized = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentException.ThrowIfNullOrWhiteSpace(baseAddress);

        var baseUri = CreateBaseUri(baseAddress);

        services.AddKeyedScoped<ApiClient<T>>(key, (serviceProvider, _) =>
            new ApiClient<T>(
                new HttpClient
                {
                    BaseAddress = baseUri,
                    Timeout = Timeout.InfiniteTimeSpan
                },
                configureRequest is null
                    ? null
                    : (request, cancellationToken) => configureRequest(serviceProvider, request, cancellationToken),
                onUnauthorized is null
                    ? null
                    : (request, cancellationToken) => onUnauthorized(serviceProvider, request, cancellationToken)));

        services.AddKeyedScoped<StreamingService<T>>(key, (serviceProvider, registeredKey) =>
            new StreamingService<T>(
                serviceProvider.GetRequiredKeyedService<ApiClient<T>>(registeredKey)));

        return services;
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