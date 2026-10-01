using System.Net.Http.Headers;
using Microsoft.Extensions.DependencyInjection;

namespace DotNetSseClient;

public static class DependencyInjection
{
    public static IServiceCollection AddSseClient<T>(
        this IServiceCollection services,
        string key,
        string baseAddress,
        Func<HttpRequestHeaders, CancellationToken, ValueTask>? configureHeaders = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentException.ThrowIfNullOrWhiteSpace(baseAddress);

        var baseUri = new Uri(baseAddress, UriKind.Absolute);
        if (baseUri.Scheme is not ("http" or "https"))
        {
            throw new ArgumentException(
                "The SSE base address must use the HTTP or HTTPS scheme.",
                nameof(baseAddress));
        }

        services.AddKeyedScoped<ApiClient<T>>(key, (_, _) =>
            new ApiClient<T>(
                new HttpClient
                {
                    BaseAddress = baseUri,
                    Timeout = Timeout.InfiniteTimeSpan
                },
                configureHeaders));

        services.AddKeyedScoped<StreamingService<T>>(key, (serviceProvider, registeredKey) =>
            new StreamingService<T>(
                serviceProvider.GetRequiredKeyedService<ApiClient<T>>(registeredKey)));

        return services;
    }
}