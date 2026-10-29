package lab;

import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SourceAssignmentTest {
    @Test void actualCallbackAndEmptyReaderMetricsAndDelegation() throws Exception {
        Map<String, Gauge<?>> gauges = new HashMap<>();
        MetricGroup group = (MetricGroup) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{MetricGroup.class},
                (p,m,a) -> { gauges.put((String)a[0], (Gauge<?>)a[1]); return a[1]; });
        List<KafkaPartitionSplit> accepted = new ArrayList<>(); List<String> calls = new ArrayList<>();
        SourceReader<String,KafkaPartitionSplit> fake = new SourceReader<>() {
            public void start() { calls.add("start"); }
            public InputStatus pollNext(ReaderOutput<String> output) { return InputStatus.NOTHING_AVAILABLE; }
            public List<KafkaPartitionSplit> snapshotState(long id) { return accepted; }
            public CompletableFuture<Void> isAvailable() { return new CompletableFuture<>(); }
            public void addSplits(List<KafkaPartitionSplit> splits) { accepted.addAll(splits); }
            public void notifyNoMoreSplits() { calls.add("noMore"); }
            public void notifyCheckpointComplete(long id) { calls.add("complete"); }
            public void notifyCheckpointAborted(long id) { calls.add("abort"); }
            public void pauseOrResumeSplits(Collection<String> p, Collection<String> r) { calls.add("pause"); }
            public void handleSourceEvents(SourceEvent event) { calls.add("event"); }
            public void close() { calls.add("close"); }
        };
        var reader = new ObservedKafkaSource.Reader(fake, group);
        assertEquals(0, gauges.get("sourceAssignedPartitionCount").getValue());
        assertTrue((Long)gauges.get("sourceAssignmentSnapshotTimestampMs").getValue() > 0);
        var split = new KafkaPartitionSplit(new TopicPartition("fixture", 5), 0);
        reader.addSplits(List.of(split)); reader.addSplits(List.of(split));
        assertEquals(1, gauges.get("sourceAssignedPartitionCount").getValue());
        assertEquals(1, gauges.get("sourceAssignedPartition5").getValue());
        assertFalse(gauges.containsKey("sourceAssignedPartition0"));
        assertSame(accepted, reader.snapshotState(1));
        assertEquals(InputStatus.NOTHING_AVAILABLE, reader.pollNext(null));
        reader.start(); reader.notifyNoMoreSplits(); reader.notifyCheckpointComplete(1);
        reader.notifyCheckpointAborted(2); reader.pauseOrResumeSplits(List.of(),List.of());
        reader.handleSourceEvents(null); reader.close();
        assertEquals(List.of("start","noMore","complete","abort","pause","event","close"),calls);
    }
}
