namespace DotNetSseClient.Tests;

public sealed class SseReconnectOptionsTests
{
    private static SseReconnectOptions NoJitter() => new()
    {
        MinimumDelay = TimeSpan.FromMilliseconds(500),
        MaxDelay = TimeSpan.FromSeconds(10),
        BackoffMultiplier = 2,
        JitterFactor = 0
    };

    [Theory]
    [InlineData(0, 1000)]
    [InlineData(1, 1000)]
    [InlineData(2, 2000)]
    [InlineData(3, 4000)]
    [InlineData(5, 10000)]
    [InlineData(500, 10000)]
    public void ComputeDelay_BacksOffExponentiallyUpToTheMaximum(int consecutiveFailures, int expectedMilliseconds)
    {
        var delay = NoJitter().ComputeDelay(TimeSpan.FromSeconds(1), consecutiveFailures, retryAfter: null);

        Assert.Equal(TimeSpan.FromMilliseconds(expectedMilliseconds), delay);
    }

    [Fact]
    public void ComputeDelay_ClampsAServerRetryOfZeroToTheMinimum()
    {
        Assert.Equal(TimeSpan.FromMilliseconds(500), NoJitter().ComputeDelay(TimeSpan.Zero, 0, null));
    }

    [Fact]
    public void ComputeDelay_HonoursALongerRetryAfter()
    {
        var options = NoJitter();

        Assert.Equal(TimeSpan.FromSeconds(30), options.ComputeDelay(TimeSpan.FromSeconds(1), 1, TimeSpan.FromSeconds(30)));
        Assert.Equal(TimeSpan.FromSeconds(1), options.ComputeDelay(TimeSpan.FromSeconds(1), 1, TimeSpan.FromMilliseconds(10)));
    }

    [Fact]
    public void ComputeDelay_AppliesJitterWithinBounds()
    {
        var options = NoJitter();
        options.JitterFactor = 0.2;

        for (var i = 0; i < 200; i++)
        {
            var delay = options.ComputeDelay(TimeSpan.FromSeconds(2), 0, null);
            Assert.InRange(delay, TimeSpan.FromSeconds(1.6), TimeSpan.FromSeconds(2.4));
        }
    }

    [Fact]
    public void ComputeDelay_KeepsAServerRetryAboveTheMaximum()
    {
        Assert.Equal(TimeSpan.FromSeconds(30), NoJitter().ComputeDelay(TimeSpan.FromSeconds(30), 3, null));
    }

    [Theory]
    [InlineData(nameof(SseReconnectOptions.InitialDelay))]
    [InlineData(nameof(SseReconnectOptions.MinimumDelay))]
    [InlineData(nameof(SseReconnectOptions.MaxDelay))]
    [InlineData(nameof(SseReconnectOptions.BackoffMultiplier))]
    [InlineData(nameof(SseReconnectOptions.JitterFactor))]
    public void Validate_RejectsInvalidValues(string property)
    {
        var options = new SseReconnectOptions();
        switch (property)
        {
            case nameof(SseReconnectOptions.InitialDelay): options.InitialDelay = TimeSpan.FromSeconds(-1); break;
            case nameof(SseReconnectOptions.MinimumDelay): options.MinimumDelay = TimeSpan.FromSeconds(-1); break;
            case nameof(SseReconnectOptions.MaxDelay): options.MaxDelay = TimeSpan.FromMilliseconds(1); break;
            case nameof(SseReconnectOptions.BackoffMultiplier): options.BackoffMultiplier = 0.5; break;
            case nameof(SseReconnectOptions.JitterFactor): options.JitterFactor = 1.5; break;
        }

        var exception = Assert.Throws<ArgumentOutOfRangeException>(options.Validate);
        Assert.Equal(property, exception.ParamName);
    }
}
