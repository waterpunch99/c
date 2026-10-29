package lab;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/** Explicit next-offset metric at source deserialization; not a committed group offset. */
public class TrackedKafkaDeserializer implements KafkaRecordDeserializationSchema<String> {
    private transient Map<Integer, Long> nextOffsets;
    private transient MetricGroup metrics;
    @Override public void open(DeserializationSchema.InitializationContext context) {
        nextOffsets = new ConcurrentHashMap<>(); metrics = context.getMetricGroup();
        // Distinct metric objects per subtask avoid query-service identity collisions.
        metrics.gauge("sourceMetricSnapshotTimestampMs", new org.apache.flink.metrics.Gauge<Long>() {
            @Override public Long getValue() { return System.currentTimeMillis(); }
        });
    }
    @Override public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<String> out) {
        if (record.value() == null) throw new IllegalArgumentException("Tombstone not allowed in benchmark schema");
        int partition = record.partition();
        if (!nextOffsets.containsKey(partition)) {
            nextOffsets.put(partition, Math.addExact(record.offset(), 1));
            metrics.gauge("sourceReadNextOffsetPartition" + partition, () -> nextOffsets.get(partition));
        } else nextOffsets.put(partition, Math.addExact(record.offset(), 1));
        out.collect(new String(record.value(), StandardCharsets.UTF_8));
    }
    @Override public TypeInformation<String> getProducedType() { return Types.STRING; }
}
