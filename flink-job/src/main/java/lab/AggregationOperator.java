package lab;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class AggregationOperator extends KeyedProcessFunction<Long, Event, Event> {
    private final int iterations, cpuSampleEvery;
    private transient ValueState<Long> counts;
    private transient AggregationMeasurements measurements;
    private transient SampledCpu cpu;
    public AggregationOperator(int iterations) { this(iterations, 0); }
    public AggregationOperator(int iterations, int cpuSampleEvery) {
        this.iterations = iterations; this.cpuSampleEvery = cpuSampleEvery;
    }
    @Override public void open(OpenContext ctx) {
        counts = getRuntimeContext().getState(new ValueStateDescriptor<>("user-event-count", Long.class));
        var runtime = getRuntimeContext();
        measurements = new AggregationMeasurements(runtime.getMetricGroup(), runtime.getTaskInfo().getIndexOfThisSubtask(),
                runtime.getTaskInfo().getMaxNumberOfParallelSubtasks());
        cpu = new SampledCpu(runtime.getMetricGroup(), cpuSampleEvery);
    }
    @Override public void processElement(Event e, Context ctx, Collector<Event> out) throws Exception {
        long start = cpu.begin();
        Long previous = counts.value();
        e.userCount = previous == null ? 1 : Math.addExact(previous, 1);
        counts.update(e.userCount);
        e.keyedCpuResult = CpuWork.compute(e.cpuResult ^ e.userCount, iterations);
        cpu.end(start);
        measurements.observe(e, System.currentTimeMillis());
        out.collect(e);
    }
}
