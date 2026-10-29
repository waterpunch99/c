package lab;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

public class HeavyOperator extends RichMapFunction<Event, Event> {
    private final int iterations, sampleEvery, cpuSampleEvery;
    private transient LogHistogram times;
    private transient SampledCpu cpu;
    public HeavyOperator(int iterations, int sampleEvery) { this(iterations, sampleEvery, 0); }
    public HeavyOperator(int iterations, int sampleEvery, int cpuSampleEvery) {
        this.iterations = iterations; this.sampleEvery = sampleEvery; this.cpuSampleEvery = cpuSampleEvery;
    }
    @Override public void open(OpenContext ctx) {
        times = new LogHistogram(getRuntimeContext().getMetricGroup(), "cpuWorkNs");
        cpu = new SampledCpu(getRuntimeContext().getMetricGroup(), cpuSampleEvery);
    }
    @Override public Event map(Event e) {
        boolean sample = sampleEvery > 0 && e.sequence % sampleEvery == 0;
        long start = sample ? System.nanoTime() : 0;
        long cpuStart = cpu.begin();
        e.cpuResult = CpuWork.compute(CpuWork.payloadSeed(e.payload, e.userId), iterations);
        cpu.end(cpuStart);
        if (sample) times.observe(System.nanoTime() - start);
        return e;
    }
}
