namespace DotNetSseClient;

/// <summary>
/// Controls how an <see cref="SseClient{T}"/> waits between reconnection attempts.
/// </summary>
/// <remarks>
/// The base delay is <see cref="InitialDelay"/>, or the server's <c>retry:</c> field once one is received.
/// Each consecutive attempt that fails before receiving an event multiplies the delay by
/// <see cref="BackoffMultiplier"/>, up to <see cref="MaxDelay"/>. Random jitter of up to
/// <see cref="JitterFactor"/> is applied, and the result is never below <see cref="MinimumDelay"/>.
/// A <c>Retry-After</c> header on a retryable response is honoured when it is longer than the computed delay.
/// </remarks>
public sealed class SseReconnectOptions
{
    /// <summary>The delay before the first reconnect, until the server sends <c>retry:</c>. Defaults to 3 seconds.</summary>
    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromSeconds(3);

    /// <summary>The lower bound for any reconnect delay, including server-provided ones. Defaults to 500 ms.</summary>
    public TimeSpan MinimumDelay { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>The upper bound for the exponential backoff. Defaults to 60 seconds.</summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>The factor applied for each consecutive failed attempt. Defaults to 2.</summary>
    public double BackoffMultiplier { get; set; } = 2;

    /// <summary>The fraction (0 to 1) of random variation applied to each delay. Defaults to 0.2.</summary>
    public double JitterFactor { get; set; } = 0.2;

    internal SseReconnectOptions Clone() => (SseReconnectOptions)MemberwiseClone();

    internal void Validate()
    {
        if (InitialDelay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(InitialDelay), InitialDelay, "Must not be negative.");
        }

        if (MinimumDelay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(MinimumDelay), MinimumDelay, "Must not be negative.");
        }

        if (MaxDelay < MinimumDelay)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxDelay), MaxDelay, "Must not be less than MinimumDelay.");
        }

        if (double.IsNaN(BackoffMultiplier) || BackoffMultiplier < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(BackoffMultiplier), BackoffMultiplier, "Must be at least 1.");
        }

        if (double.IsNaN(JitterFactor) || JitterFactor is < 0 or > 1)
        {
            throw new ArgumentOutOfRangeException(nameof(JitterFactor), JitterFactor, "Must be between 0 and 1.");
        }
    }

    internal TimeSpan ComputeDelay(TimeSpan baseDelay, int consecutiveFailures, TimeSpan? retryAfter)
    {
        var floor = baseDelay < MinimumDelay ? MinimumDelay : baseDelay;
        var ceiling = MaxDelay > floor ? MaxDelay : floor;

        var milliseconds = floor.TotalMilliseconds;
        if (consecutiveFailures > 1)
        {
            milliseconds *= Math.Pow(BackoffMultiplier, consecutiveFailures - 1);
        }

        milliseconds = Math.Min(milliseconds, ceiling.TotalMilliseconds);

        if (JitterFactor > 0)
        {
            milliseconds *= 1 + ((Random.Shared.NextDouble() * 2) - 1) * JitterFactor;
        }

        milliseconds = Math.Max(milliseconds, MinimumDelay.TotalMilliseconds);
        var delay = TimeSpan.FromMilliseconds(milliseconds);

        return retryAfter is { } serverDelay && serverDelay > delay ? serverDelay : delay;
    }
}
