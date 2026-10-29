package lab;

import java.util.BitSet;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

/** No external writes. Counters are observations, not transactional exactly-once results. */
public class MeasurementSink implements Sink<Event> {
    private final int sampleEvery, verificationRecords;
    public MeasurementSink(int sampleEvery, int verificationRecords) {
        this.sampleEvery = sampleEvery; this.verificationRecords = verificationRecords;
    }
    @Override public SinkWriter<Event> createWriter(WriterInitContext ctx) {
        return new Writer(ctx.metricGroup(), sampleEvery, verificationRecords);
    }
    static class Writer implements SinkWriter<Event> {
        private final Counter completed, measurement, warmup, checksum, sequenceSum, duplicates, invalidLatency;
        private final Counter windowCompleted, windowCohortCompleted, hotRecords;
        private final LogHistogram latency;
        private final int sampleEvery, verificationRecords;
        private final BitSet seen;
        Writer(MetricGroup group, int sampleEvery, int verificationRecords) {
            this.sampleEvery = sampleEvery; this.verificationRecords = verificationRecords;
            completed = group.counter("completedRecords"); measurement = group.counter("measurementCompletedRecords");
            warmup = group.counter("warmupCompletedRecords"); checksum = group.counter("resultChecksum");
            sequenceSum = group.counter("sequenceSum"); duplicates = group.counter("duplicateRecords");
            invalidLatency = group.counter("negativeLatencySamples");
            windowCompleted = group.counter("measurementWindowCompletedRecords");
            windowCohortCompleted = group.counter("measurementWindowCohortCompletedRecords");
            hotRecords = group.counter("hotKeyCompletedRecords");
            latency = new LogHistogram(group, "e2eLatencyMs");
            group.gauge("sinkMetricSnapshotTimestampMs", new org.apache.flink.metrics.Gauge<Long>() {
                @Override public Long getValue() { return System.currentTimeMillis(); }
            });
            seen = verificationRecords > 0 ? new BitSet(verificationRecords) : null;
            // Bounded Smoke audit: reconstruct the exact ID set across all sink subtasks.
            for (int word = 0; word < (verificationRecords + 63) / 64; word++) {
                final int first = word * 64;
                group.gauge("verificationSeenWord" + word, () -> {
                    long[] bits = seen.get(first, Math.min(first + 64, verificationRecords)).toLongArray();
                    return bits.length == 0 ? 0L : bits[0];
                });
            }
        }
        @Override public void write(Event e, Context context) {
            observe(e, System.currentTimeMillis());
        }
        void observe(Event e, long now) {
            if (seen != null) {
                if (e.sequence >= verificationRecords) throw new IllegalArgumentException("Verification range exceeded");
                int id = (int) e.sequence;
                if (seen.get(id)) duplicates.inc(); else seen.set(id);
            }
            completed.inc(); sequenceSum.inc(e.sequence);
            checksum.inc(e.cpuResult ^ e.keyedCpuResult ^ e.userCount);
            if ("measurement".equals(e.generationPhase)) measurement.inc(); else warmup.inc();
            if (e.userId == 1) hotRecords.inc();
            if (e.measurementStartTimestamp > 0 && now >= e.measurementStartTimestamp && now < e.measurementEndTimestamp) {
                windowCompleted.inc();
                if ("measurement".equals(e.generationPhase)) windowCohortCompleted.inc();
            }
            if (sampleEvery > 0 && e.sequence % sampleEvery == 0 && "measurement".equals(e.generationPhase)) {
                long ms = now - e.producerTimestamp;
                if (ms < 0) invalidLatency.inc(); else latency.observe(ms);
            }
        }
        @Override public void flush(boolean endOfInput) {}
        @Override public void close() {}
    }
}
