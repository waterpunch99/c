package lab;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class JobOptions {
    private static final Set<String> ALLOWED = Set.of("bootstrap-servers", "topic", "group-id",
            "experiment-id", "run-id", "source-parallelism", "parse-parallelism", "heavy-parallelism",
            "aggregation-parallelism", "sink-parallelism", "max-parallelism", "cpu-work-iterations",
            "keyed-cpu-work-iterations", "checkpoint-interval-ms", "checkpoint-timeout-ms",
            "checkpoint-path", "state-backend", "chaining", "latency-sample-every", "verification-records", "operator-cpu-sample-every");
    private final Map<String, String> values;
    public final String experiment, run, topic, group, bootstrap, backend, checkpointPath;
    public final int sourceP, parseP, heavyP, aggregationP, sinkP, maxP, cpuIterations, keyedIterations,
            sampleEvery, verificationRecords, cpuSampleEvery;
    public final long checkpointInterval, checkpointTimeout;
    public final boolean chaining;

    public JobOptions(String... args) {
        values = new HashMap<>();
        if (args.length % 2 != 0) throw new IllegalArgumentException("Expected --option value pairs");
        for (int i = 0; i < args.length; i += 2) {
            if (!args[i].startsWith("--")) throw new IllegalArgumentException("Expected --option");
            String key = args[i].substring(2);
            if (!ALLOWED.contains(key) || values.putIfAbsent(key, args[i + 1]) != null)
                throw new IllegalArgumentException("Unknown or duplicate option: " + key);
        }
        experiment = required("experiment-id"); run = required("run-id");
        if (!experiment.matches("[A-Za-z0-9_-]+") || !run.matches("[A-Za-z0-9_-]+"))
            throw new IllegalArgumentException("Identifiers must contain only letters, digits, _, -");
        topic = required("topic"); group = required("group-id");
        bootstrap = values.getOrDefault("bootstrap-servers", "kafka:29092");
        maxP = integer("max-parallelism", 128, 1, 32768);
        sourceP = integer("source-parallelism", 2, 1, maxP);
        parseP = integer("parse-parallelism", 2, 1, maxP);
        heavyP = integer("heavy-parallelism", 2, 1, maxP);
        aggregationP = integer("aggregation-parallelism", 2, 1, maxP);
        sinkP = integer("sink-parallelism", 2, 1, maxP);
        cpuIterations = integer("cpu-work-iterations", 5000, 0, Integer.MAX_VALUE);
        keyedIterations = integer("keyed-cpu-work-iterations", 3000, 0, Integer.MAX_VALUE);
        sampleEvery = integer("latency-sample-every", 100, 0, Integer.MAX_VALUE);
        cpuSampleEvery = integer("operator-cpu-sample-every", 0, 0, Integer.MAX_VALUE);
        verificationRecords = integer("verification-records", 0, 0, 10000);
        checkpointInterval = number("checkpoint-interval-ms", 10000, 0, Long.MAX_VALUE);
        checkpointTimeout = number("checkpoint-timeout-ms", 60000, 1, Long.MAX_VALUE);
        backend = values.getOrDefault("state-backend", "hashmap");
        if (!Set.of("hashmap", "rocksdb").contains(backend)) throw new IllegalArgumentException("Unsupported state-backend");
        checkpointPath = values.getOrDefault("checkpoint-path", "file:///checkpoints/" + run);
        String chain = values.getOrDefault("chaining", "false");
        if (!Set.of("true", "false").contains(chain)) throw new IllegalArgumentException("chaining must be true/false");
        chaining = Boolean.parseBoolean(chain);
    }
    private String required(String key) {
        String v = values.get(key);
        if (v == null || v.isBlank()) throw new IllegalArgumentException("Missing --" + key);
        return v;
    }
    private long number(String key, long defaultValue, long min, long max) {
        long n = Long.parseLong(values.getOrDefault(key, Long.toString(defaultValue)));
        if (n < min || n > max) throw new IllegalArgumentException("Out of range: " + key);
        return n;
    }
    private int integer(String key, int defaultValue, int min, int max) {
        return (int) number(key, defaultValue, min, max);
    }
}
