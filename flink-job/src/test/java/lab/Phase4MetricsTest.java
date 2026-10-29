package lab;

import java.lang.management.ThreadMXBean;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import org.apache.flink.metrics.*;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class Phase4MetricsTest {
    final HashMap<String, Counter> counters = new HashMap<>();
    final HashMap<String, Gauge<?>> gauges = new HashMap<>();
    MetricGroup group() {
        return (MetricGroup) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{MetricGroup.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("counter")) return counters.computeIfAbsent((String)args[0], k -> new SimpleCounter());
                    if (method.getName().equals("gauge")) { gauges.put((String)args[0], (Gauge<?>)args[1]); return args[1]; }
                    throw new UnsupportedOperationException(method.getName());
                });
    }
    @Test void cohortWindowAndActualHotPlacement() {
        var m = new AggregationMeasurements(group(), 3, 128);
        assertEquals(-1, gauges.get("hotKeyObservedSubtask").getValue());
        Event e = new Event(); e.userId = 2; e.generationPhase = "warmup";
        m.observe(e, 1200);
        assertEquals(0, counters.get("aggregationMeasurementRecords").getCount());
        e.userId = 1; e.generationPhase = "measurement";
        e.measurementStartTimestamp = 1000; e.measurementEndTimestamp = 2000;
        m.observe(e, 1999); m.observe(e, 2000);
        e.userId = 2; m.observe(e, 1500);
        assertEquals(3, counters.get("aggregationMeasurementRecords").getCount());
        assertEquals(2, counters.get("aggregationMeasurementHotRecords").getCount());
        assertEquals(2, counters.get("aggregationMeasurementWindowRecords").getCount());
        assertEquals(1, counters.get("aggregationMeasurementWindowHotRecords").getCount());
        assertEquals(3, gauges.get("hotKeyObservedSubtask").getValue());
        assertEquals(KeyGroupRangeAssignment.assignToKeyGroup(1L, 128), gauges.get("hotKeyObservedKeyGroup").getValue());
    }
    @Test void sampledCpuUsesLocalRecordSamplingAndRetainsInvalidRead() {
        long[] time = {100, 150, 200, -1}; int[] i = {0};
        var bean = (ThreadMXBean) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{ThreadMXBean.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "isCurrentThreadCpuTimeSupported", "isThreadCpuTimeEnabled" -> true;
                    case "getCurrentThreadCpuTime" -> time[i[0]++];
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        var cpu = new SampledCpu(group(), 2, bean);
        assertEquals(-1, cpu.begin()); cpu.end(cpu.begin());
        assertEquals(-1, cpu.begin()); cpu.end(cpu.begin());
        assertEquals(1, counters.get("operatorCpuSampleCount").getCount());
        assertEquals(50, counters.get("sampledOperatorCpuNanos").getCount());
        assertEquals(1, counters.get("operatorCpuInvalidSamples").getCount());
    }
    @Test void unsupportedCpuIsExplicitAndMakesNoMeasurements() {
        var bean = (ThreadMXBean) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{ThreadMXBean.class},
                (proxy, method, args) -> false);
        var cpu = new SampledCpu(group(), 1, bean);
        assertEquals(-1, cpu.begin());
        assertEquals(0, gauges.get("operatorCpuTimeSupported").getValue());
        assertEquals(0, counters.get("operatorCpuSampleCount").getCount());
    }
}
