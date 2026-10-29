package lab;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

/** Samples only the synchronous operator body, not task/thread/process utilization. */
final class SampledCpu {
    private final ThreadMXBean bean;
    private final int every;
    private final boolean supported;
    private final Counter nanos, samples, invalid;
    private long records;
    SampledCpu(MetricGroup group, int every) {
        this(group, every, ManagementFactory.getThreadMXBean());
    }
    SampledCpu(MetricGroup group, int every, ThreadMXBean bean) {
        this.bean = bean; this.every = every;
        boolean available = false;
        if (every > 0) {
            try {
                if (bean.isCurrentThreadCpuTimeSupported()) {
                    if (!bean.isThreadCpuTimeEnabled()) bean.setThreadCpuTimeEnabled(true);
                    available = bean.isThreadCpuTimeEnabled();
                }
            } catch (UnsupportedOperationException | SecurityException ignored) { }
        }
        supported = available;
        nanos = group.counter("sampledOperatorCpuNanos");
        samples = group.counter("operatorCpuSampleCount");
        invalid = group.counter("operatorCpuInvalidSamples");
        group.gauge("operatorCpuTimeSupported", new Gauge<Integer>() {
            @Override public Integer getValue() { return supported ? 1 : 0; }
        });
    }
    long begin() {
        return every > 0 && ++records % every == 0 && supported ? bean.getCurrentThreadCpuTime() : -1;
    }
    void end(long start) {
        if (start < 0) return;
        long end = bean.getCurrentThreadCpuTime();
        if (end < start) { invalid.inc(); return; }
        nanos.inc(end - start); samples.inc();
    }
}
