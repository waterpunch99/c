package lab;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

/** Actual processed measurement cohort and observed hot-key placement, never a predicted assignment. */
final class AggregationMeasurements {
    private final Counter cohort, hot, window, windowHot;
    private final int subtask, maxParallelism;
    private boolean hotObserved;
    AggregationMeasurements(MetricGroup group, int subtask, int maxParallelism) {
        this.subtask = subtask; this.maxParallelism = maxParallelism;
        cohort = group.counter("aggregationMeasurementRecords");
        hot = group.counter("aggregationMeasurementHotRecords");
        window = group.counter("aggregationMeasurementWindowRecords");
        windowHot = group.counter("aggregationMeasurementWindowHotRecords");
        group.gauge("hotKeyObservedSubtask", new Gauge<Integer>() {
            @Override public Integer getValue() { return hotObserved ? AggregationMeasurements.this.subtask : -1; }
        });
        group.gauge("hotKeyObservedKeyGroup", new Gauge<Integer>() {
            @Override public Integer getValue() {
                return hotObserved ? KeyGroupRangeAssignment.assignToKeyGroup(1L, AggregationMeasurements.this.maxParallelism) : -1;
            }
        });
        group.gauge("aggregationMetricObservedWallMs", new Gauge<Long>() {
            @Override public Long getValue() { return System.currentTimeMillis(); }
        });
    }
    void observe(Event e, long now) {
        if (e.userId == 1) hotObserved = true;
        if (!"measurement".equals(e.generationPhase)) return;
        cohort.inc();
        if (e.userId == 1) hot.inc();
        if (e.measurementStartTimestamp <= now && now < e.measurementEndTimestamp) {
            window.inc();
            if (e.userId == 1) windowHot.inc();
        }
    }
}
