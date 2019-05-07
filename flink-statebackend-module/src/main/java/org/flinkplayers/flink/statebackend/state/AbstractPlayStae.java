package org.flinkplayers.flink.statebackend.state;

import org.apache.flink.api.common.state.State;
import org.apache.flink.runtime.state.internal.InternalKvState;

public abstract class AbstractPlayStae<K, N, V> implements InternalKvState<K, N, V>, State {
}
