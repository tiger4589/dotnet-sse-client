using System.Net;

namespace DotNetSseClient;

/// <summary>
/// Raised when an SSE stream stops permanently and will not reconnect, for example after a
/// non-retryable HTTP status, a response that is not <c>text/event-stream</c>, or a failure in a
/// request or authorization callback.
/// </summary>
/// <remarks>
/// It is passed to the <c>onError</c> callback and faults <see cref="SseClient{T}.Completion"/>.
/// Errors that are followed by a reconnect are reported with their original exception type instead.
/// </remarks>
public sealed class SseStreamTerminatedException : Exception
{
    /// <summary>Creates a new instance.</summary>
    public SseStreamTerminatedException(string message, Exception? innerException = null, HttpStatusCode? statusCode = null)
        : base(message, innerException)
    {
        StatusCode = statusCode;
    }

    /// <summary>The HTTP status that ended the stream, when the cause was an HTTP response.</summary>
    public HttpStatusCode? StatusCode { get; }
}
