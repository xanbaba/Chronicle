namespace Chronicle.Postgres.Tests;

public static class DateTimeExtensions
{
    public static DateTime TruncateToSeconds(this DateTime dateTime)
    {
        return dateTime.AddTicks(-(dateTime.Ticks % TimeSpan.TicksPerSecond));
    }
}