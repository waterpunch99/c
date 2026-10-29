package lab;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.api.common.typeinfo.Types;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class OperatorsTest {
    private Event event(long sequence, long user) {
        Event e = new Event(); e.eventId = "run:" + sequence; e.sequence = sequence; e.userId = user;
        e.runId = "run"; e.experimentId = "exp"; e.producerTimestamp = 1000;
        e.payload = "abc"; e.generationPhase = "measurement";
        return e;
    }
    @Test void cpuMatchesIndependentReference() {
        for (long seed : new long[]{0, 1, -1, Long.MAX_VALUE, 123456789}) {
            for (int iterations : new int[]{0, 1, 17, 5000}) {
                long ref = seed;
                for (int i = 0; i < iterations; i++) {
                    ref = ref ^ (ref >>> 12); ref = ref ^ (ref << 25); ref = ref ^ (ref >>> 27);
                    long y = ref * 2685821657736338717L + i;
                    ref = (y << 17) | (y >>> 47);
                }
                assertEquals(ref, CpuWork.compute(seed, iterations));
            }
        }
        assertEquals(123L, CpuWork.compute(123, 0));
        assertNotEquals(CpuWork.payloadSeed("abc", 1), CpuWork.payloadSeed("abd", 1));
        assertThrows(IllegalArgumentException.class, () -> CpuWork.compute(1, -1));
    }
    @Test void parseRoundtripAndRejectCorruption() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Event e = ParseOperator.parse(mapper, mapper.writeValueAsString(event(0, 1)), "exp", "run");
        assertEquals("abc", e.payload);
        assertThrows(Exception.class, () -> ParseOperator.parse(mapper, "broken", "exp", "run"));
        assertThrows(Exception.class, () -> ParseOperator.parse(mapper, "{}", "exp", "run"));
        assertThrows(Exception.class, () -> ParseOperator.parse(mapper, mapper.writeValueAsString(event(0, 1)), "exp", "other"));
    }
    @Test void aggregationKeepsIndependentKeysAcrossSnapshotRestore() throws Exception {
        var op = new KeyedProcessOperator<Long, Event, Event>(new AggregationOperator(3));
        var snapshot = runAndSnapshot(op);
        try (var h = harness(new KeyedProcessOperator<>(new AggregationOperator(3)))) {
            h.setup(); h.initializeState(snapshot); h.open();
            Event e = event(4, 1); h.processElement(new StreamRecord<>(e));
            assertEquals(3, e.userCount);
            Event other = event(5, 2); h.processElement(new StreamRecord<>(other));
            assertEquals(2, other.userCount);
            assertEquals(2, h.extractOutputStreamRecords().size());
        }
    }
    private org.apache.flink.runtime.checkpoint.OperatorSubtaskState runAndSnapshot(KeyedProcessOperator<Long, Event, Event> op) throws Exception {
        try (var h = harness(op)) {
            h.open();
            Event a = event(0, 1), b = event(1, 2), c = event(2, 1);
            h.processElement(new StreamRecord<>(a)); h.processElement(new StreamRecord<>(b)); h.processElement(new StreamRecord<>(c));
            assertEquals(1, a.userCount); assertEquals(1, b.userCount); assertEquals(2, c.userCount);
            assertEquals(CpuWork.compute(2, 3), c.keyedCpuResult);
            assertEquals(3, h.extractOutputStreamRecords().size());
            return h.snapshot(1, 1000);
        }
    }
    private KeyedOneInputStreamOperatorTestHarness<Long, Event, Event> harness(KeyedProcessOperator<Long, Event, Event> op) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(op, e -> e.userId, Types.LONG);
    }
    @Test void optionsRejectInvalidParameters() {
        String[] base = {"--experiment-id", "exp", "--run-id", "run", "--topic", "topic", "--group-id", "group"};
        assertEquals(128, new JobOptions(base).maxP);
        assertFalse(new JobOptions(base).chaining);
        assertThrows(IllegalArgumentException.class, () -> new JobOptions("--topic", "x"));
        assertThrows(IllegalArgumentException.class, () -> new JobOptions("--unknown", "1"));
        assertThrows(IllegalArgumentException.class, () -> new JobOptions("--topic", "a", "--topic", "b"));
        var tooHigh = java.util.Arrays.copyOf(base, base.length + 2);
        tooHigh[base.length] = "--heavy-parallelism"; tooHigh[base.length + 1] = "129";
        assertThrows(IllegalArgumentException.class, () -> new JobOptions(tooHigh));
    }

    @Test void sinkCountsCompletionAndDetectsBoundedDuplicates() throws Exception {
        var counters = new java.util.HashMap<String, org.apache.flink.metrics.Counter>();
        var gauges = new java.util.HashMap<String, org.apache.flink.metrics.Gauge<?>>();
        var group = (org.apache.flink.metrics.MetricGroup) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{org.apache.flink.metrics.MetricGroup.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("counter"))
                        return counters.computeIfAbsent((String) args[0], k -> new org.apache.flink.metrics.SimpleCounter());
                    if (method.getName().equals("gauge")) {
                        gauges.put((String) args[0], (org.apache.flink.metrics.Gauge<?>) args[1]);
                        return args[1];
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        var writer = new MeasurementSink.Writer(group, 1, 10);
        Event e = event(0, 1); e.producerTimestamp = System.currentTimeMillis() - 100;
        e.cpuResult = 123; e.keyedCpuResult = 456; e.userCount = 1;
        writer.write(e, null); writer.write(e, null);
        assertEquals(2, counters.get("completedRecords").getCount());
        assertEquals(2, counters.get("measurementCompletedRecords").getCount());
        assertEquals(1, counters.get("duplicateRecords").getCount());
        assertEquals(1L, gauges.get("verificationSeenWord0").getValue());
        assertEquals(2 * (123L ^ 456L ^ 1L), counters.get("resultChecksum").getCount());
        assertEquals(2, counters.get("e2eLatencyMsSampleCount").getCount());
        assertThrows(IllegalArgumentException.class, () -> writer.write(event(10, 1), null));
        Event negative = event(1, 1); negative.producerTimestamp = System.currentTimeMillis() + 60000;
        writer.write(negative, null);
        assertEquals(1, counters.get("negativeLatencySamples").getCount());
        Event warmup = event(2, 1); warmup.generationPhase = "warmup";
        warmup.measurementStartTimestamp = 1000; warmup.measurementEndTimestamp = 2000;
        writer.observe(warmup, 1500);
        Event measured = event(3, 1); measured.measurementStartTimestamp = 1000; measured.measurementEndTimestamp = 2000;
        writer.observe(measured, 1900);
        Event drained = event(4, 1); drained.measurementStartTimestamp = 1000; drained.measurementEndTimestamp = 2000;
        writer.observe(drained, 2000);
        assertEquals(2, counters.get("measurementWindowCompletedRecords").getCount());
        assertEquals(1, counters.get("measurementWindowCohortCompletedRecords").getCount());
        writer.flush(false); writer.close();
    }
}
