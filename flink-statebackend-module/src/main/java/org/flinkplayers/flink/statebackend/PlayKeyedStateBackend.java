package org.flinkplayers.flink.statebackend;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FlinkRuntimeException;
import org.flinkplayers.flink.statebackend.connection.PlayConnection;
import org.flinkplayers.flink.statebackend.state.PlayValueState;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PlayKeyedStateBackend<K> extends AbstractKeyedStateBackend<K> {

    private PlayStateBackend playStateBackend;

    private interface StateFactory {
        <K, N, SV, S extends State, IS extends S> IS createState(
                StateDescriptor<S, SV> stateDesc,
                Tuple2<PlayConnection, RegisteredKeyValueStateBackendMetaInfo<N, SV>> registerResult,
                PlayKeyedStateBackend<K> backend) throws Exception;
    }

    private static final Map<Object, StateFactory> STATE_FACTORIES = Stream.of(
            Tuple2.of(ValueStateDescriptor.class, (StateFactory) PlayValueState::create)
    ).collect(Collectors.toMap(t -> t.f0, t -> t.f1));

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
    public <N, SV, SEV, S extends State, IS extends S> IS createInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory
    ) throws Exception {
        StateFactory stateFactory = STATE_FACTORIES.get(stateDesc.getClass());
        if (stateFactory == null) {
            String message = String.format("State %s is not supported by %s",
                    stateDesc.getClass(), this.getClass());
            throw new FlinkRuntimeException(message);
        }
        Tuple2<PlayConnection, RegisteredKeyValueStateBackendMetaInfo<N, SV>> registerResult = tryRegisterKvStateInformation(
                stateDesc, namespaceSerializer, snapshotTransformFactory);
        return stateFactory.createState(stateDesc, registerResult, PlayKeyedStateBackend.this);
    }

    private  <N, S extends State, SV, SEV> Tuple2<PlayConnection, RegisteredKeyValueStateBackendMetaInfo<N, SV>> tryRegisterKvStateInformation(
            StateDescriptor<S, SV> stateDesc,
            TypeSerializer<N> namespaceSerializer,
            StateSnapshotTransformer.StateSnapshotTransformFactory<SEV> snapshotTransformFactory
    ) {
        System.out.println("stateDesc = [" + stateDesc + "], namespaceSerializer = [" + namespaceSerializer + "], snapshotTransformFactory = [" + snapshotTransformFactory + "]");

        // TODO Create Tuple2<PlayConnection, RegisteredKeyValueStateBackendMetaInfo<N, SV>>

        //return Tuple2.of(newRocksStateInfo.columnFamilyHandle, newMetaInfo);
        //
        // return Tuple2.of(playStateBackend.getPlayConnection(),newMetaInfo);
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


    public void setPlayStateBackend(PlayStateBackend playStateBackend) {
        this.playStateBackend = playStateBackend;
    }
}
