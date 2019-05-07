package org.flinkplayers.flink.statebackend.state;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.flinkplayers.flink.statebackend.PlayKeyedStateBackend;
import org.flinkplayers.flink.statebackend.connection.PlayConnection;

public class PlayValueState<K, N, V> extends AbstractPlayStae<K, N, V> implements InternalKvState<K, N, V> ,State{

    private PlayConnection handle;
    private TypeSerializer<N> namespaceSerializer;
    private TypeSerializer<V> valueSerializer;
    private V defaultValue;
    private PlayKeyedStateBackend<K> backend;

    public PlayValueState(
            PlayConnection handle,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<V> valueSerializer,
            V defaultValue,
            PlayKeyedStateBackend<K> backend) {
        this.handle = handle;
        this.namespaceSerializer = namespaceSerializer;
        this.valueSerializer = valueSerializer;
        this.defaultValue = defaultValue;
        this.backend = backend;
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return null;
    }

    @Override
    public TypeSerializer<N> getNamespaceSerializer() {
        return null;
    }

    @Override
    public TypeSerializer<V> getValueSerializer() {
        return null;
    }

    @Override
    public void setCurrentNamespace(N namespace) {

    }

    @Override
    public byte[] getSerializedValue(byte[] serializedKeyAndNamespace, TypeSerializer<K> safeKeySerializer, TypeSerializer<N> safeNamespaceSerializer, TypeSerializer<V> safeValueSerializer) throws Exception {
        return new byte[0];
    }

    @Override
    public StateIncrementalVisitor<K, N, V> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
        return null;
    }

    @Override
    public void clear() {

    }

    @SuppressWarnings("unchecked")
    public static <K, N, SV, S extends State, IS extends S> IS create(
            StateDescriptor<S, SV> stateDesc,
            Tuple2<PlayConnection, RegisteredKeyValueStateBackendMetaInfo<N, SV>> registerResult,
            PlayKeyedStateBackend<K> backend) {
        return (IS) new PlayValueState<>(
                registerResult.f0,
                registerResult.f1.getNamespaceSerializer(),
                registerResult.f1.getStateSerializer(),
                stateDesc.getDefaultValue(),
                backend);
    }
}
