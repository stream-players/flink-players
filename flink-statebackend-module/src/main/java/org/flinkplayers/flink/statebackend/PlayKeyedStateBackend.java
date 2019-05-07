package org.flinkplayers.flink.statebackend;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import javax.annotation.Nonnull;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Stream;

public class PlayKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {
    public PlayKeyedStateBackend(TaskKvStateRegistry kvStateRegistry, TypeSerializer<K> keySerializer, ClassLoader userCodeClassLoader, int numberOfKeyGroups, KeyGroupRange keyGroupRange, ExecutionConfig executionConfig, TtlTimeProvider ttlTimeProvider, CloseableRegistry cancelStreamRegistry) {
        super(kvStateRegistry, keySerializer, userCodeClassLoader, numberOfKeyGroups, keyGroupRange, executionConfig, ttlTimeProvider, cancelStreamRegistry);
    }

    public PlayKeyedStateBackend(TaskKvStateRegistry kvStateRegistry, StateSerializerProvider<K> keySerializerProvider, ClassLoader userCodeClassLoader, int numberOfKeyGroups, KeyGroupRange keyGroupRange, ExecutionConfig executionConfig, TtlTimeProvider ttlTimeProvider, CloseableRegistry cancelStreamRegistry, StreamCompressionDecorator keyGroupCompressionDecorator) {
        super(kvStateRegistry, keySerializerProvider, userCodeClassLoader, numberOfKeyGroups, keyGroupRange, executionConfig, ttlTimeProvider, cancelStreamRegistry, keyGroupCompressionDecorator);
    }

    @Override
    public int numKeyValueStateEntries() {
        return 0;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }

    @Override
    public <N> Stream<K> getKeys(String state, N namespace) {
        return null;
    }

    @Nonnull
    @Override
    public <N, SV, SEV, S extends State, IS extends S> IS createInternalState(@Nonnull TypeSerializer<N> namespaceSerializer, @Nonnull StateDescriptor<S, SV> stateDesc, @Nonnull StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory) throws Exception {
        return null;
    }

    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable & Keyed> KeyGroupedInternalPriorityQueue<T> create(@Nonnull String stateName, @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
        return null;
    }

    @Nonnull
    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(long checkpointId, long timestamp, @Nonnull CheckpointStreamFactory streamFactory, @Nonnull CheckpointOptions checkpointOptions) throws Exception {
        return null;
    }
}
