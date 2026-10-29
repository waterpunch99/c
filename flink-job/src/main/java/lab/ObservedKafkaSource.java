package lab;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumState;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;

/** Assignment callbacks, not inferred hashes/read history. Only fixed-topic unbounded runs. */
public final class ObservedKafkaSource implements Source<String, KafkaPartitionSplit, KafkaSourceEnumState>, ResultTypeQueryable<String> {
    private final KafkaSource<String> delegate;
    public ObservedKafkaSource(KafkaSource<String> delegate) {
        if (delegate.getBoundedness() != Boundedness.CONTINUOUS_UNBOUNDED)
            throw new IllegalArgumentException("Assignment adapter requires unbounded source");
        this.delegate = delegate;
    }
    @Override public Boundedness getBoundedness() { return delegate.getBoundedness(); }
    @Override public TypeInformation<String> getProducedType() { return Types.STRING; }
    @Override public SourceReader<String, KafkaPartitionSplit> createReader(SourceReaderContext context) throws Exception {
        return new Reader(delegate.createReader(context), context.metricGroup());
    }
    @Override public SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> createEnumerator(SplitEnumeratorContext<KafkaPartitionSplit> context) throws Exception { return delegate.createEnumerator(context); }
    @Override public SplitEnumerator<KafkaPartitionSplit, KafkaSourceEnumState> restoreEnumerator(SplitEnumeratorContext<KafkaPartitionSplit> context, KafkaSourceEnumState state) throws Exception { return delegate.restoreEnumerator(context, state); }
    @Override public SimpleVersionedSerializer<KafkaPartitionSplit> getSplitSerializer() { return delegate.getSplitSerializer(); }
    @Override public SimpleVersionedSerializer<KafkaSourceEnumState> getEnumeratorCheckpointSerializer() { return delegate.getEnumeratorCheckpointSerializer(); }

    static final class Reader implements SourceReader<String, KafkaPartitionSplit> {
        private final SourceReader<String, KafkaPartitionSplit> delegate;
        private final Set<Integer> assigned = ConcurrentHashMap.newKeySet();
        private final MetricGroup metrics;
        Reader(SourceReader<String, KafkaPartitionSplit> delegate, MetricGroup metrics) {
            this.delegate = delegate; this.metrics = metrics;
            metrics.gauge("sourceAssignedPartitionCount", () -> assigned.size());
            metrics.gauge("sourceAssignmentSnapshotTimestampMs", new org.apache.flink.metrics.Gauge<Long>() {
                @Override public Long getValue() { return System.currentTimeMillis(); }
            });
        }
        @Override public void addSplits(List<KafkaPartitionSplit> splits) {
            // Forward first: failed assignment must not be reported as accepted.
            delegate.addSplits(splits);
            for (KafkaPartitionSplit split : splits) {
                int p = split.getTopicPartition().partition();
                if (assigned.add(p)) metrics.gauge("sourceAssignedPartition" + p, new org.apache.flink.metrics.Gauge<Integer>() {
                    @Override public Integer getValue() { return 1; }
                });
            }
        }
        @Override public void start() { delegate.start(); }
        @Override public InputStatus pollNext(ReaderOutput<String> output) throws Exception { return delegate.pollNext(output); }
        @Override public List<KafkaPartitionSplit> snapshotState(long id) { return delegate.snapshotState(id); }
        @Override public CompletableFuture<Void> isAvailable() { return delegate.isAvailable(); }
        @Override public void notifyNoMoreSplits() { delegate.notifyNoMoreSplits(); }
        @Override public void handleSourceEvents(SourceEvent event) { delegate.handleSourceEvents(event); }
        @Override public void notifyCheckpointComplete(long id) throws Exception { delegate.notifyCheckpointComplete(id); }
        @Override public void notifyCheckpointAborted(long id) throws Exception { delegate.notifyCheckpointAborted(id); }
        @Override public void pauseOrResumeSplits(Collection<String> pause, Collection<String> resume) { delegate.pauseOrResumeSplits(pause, resume); }
        @Override public void close() throws Exception { delegate.close(); }
    }
}
