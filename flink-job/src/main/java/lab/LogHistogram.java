package lab;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

/** Mergeable log2 buckets; values are rounded to an upper bound, never averaged percentiles. */
public final class LogHistogram {
    private final Counter[] buckets = new Counter[64];
    private final Counter count, sum;
    private long min = Long.MAX_VALUE, max;
    public LogHistogram(MetricGroup group, String name) {
        count = group.counter(name + "SampleCount");
        sum = group.counter(name + "Sum");
        for (int i = 0; i < buckets.length; i++) buckets[i] = group.counter(name + "Bucket" + i);
        group.gauge(name + "Min", () -> count.getCount() == 0 ? -1L : min);
        group.gauge(name + "Max", () -> count.getCount() == 0 ? -1L : max);
    }
    public void observe(long value) {
        if (value < 0) throw new IllegalArgumentException("Negative histogram value");
        int b = value == 0 ? 0 : 64 - Long.numberOfLeadingZeros(value);
        buckets[Math.min(b, 63)].inc(); count.inc(); sum.inc(value);
        min = Math.min(min, value); max = Math.max(max, value);
    }
}
