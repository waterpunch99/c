package lab;

import java.io.Serializable;

/** Mutable POJO: one input record produces one output record. */
public class Event implements Serializable {
    public String eventId;
    public long userId;
    public long producerTimestamp;
    public String payload;
    public String experimentId;
    public String runId;
    public String generationPhase;
    public long sequence;
    public long measurementStartTimestamp;
    public long measurementEndTimestamp;
    public long cpuResult;
    public long keyedCpuResult;
    public long userCount;

    public Event() {}

    public void validate(String experiment, String run) {
        if (userId < 1 || producerTimestamp <= 0 || payload == null || sequence < 0
                || (measurementStartTimestamp != 0 && measurementEndTimestamp <= measurementStartTimestamp)
                || !experiment.equals(experimentId) || !run.equals(runId)
                || !((run + ":" + sequence).equals(eventId))
                || !("warmup".equals(generationPhase) || "measurement".equals(generationPhase))) {
            throw new IllegalArgumentException("Invalid schema or foreign run event");
        }
    }
}
