package org.flinkplayers.flink.statebackend;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.flinkplayers.flink.statebackend.connection.PlayConnection;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;

public class PlayStateBackend implements StateBackend, ConfigurableStateBackend {

    private Configuration configuration;
    private ClassLoader classLoader;

    private PlayConnection playConnection;

    public PlayStateBackend() {
    }

    public PlayStateBackend(Configuration configuration, ClassLoader classLoader) {
        this.configuration = configuration;
        this.classLoader = classLoader;
    }

    @Override
    public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) throws IOException {
        return null;
    }

    @Override
    public CheckpointStorage createCheckpointStorage(JobID jobId) throws IOException {
        return null;
    }

    @Override
    public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
            Environment env,
            JobID jobID,
            String operatorIdentifier,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            TaskKvStateRegistry kvStateRegistry,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry) {
        PlayKeyedStateBackend<K> playKeyedStateBackend = new PlayKeyedStateBackend<>(
                env.getTaskKvStateRegistry(),
                keySerializer,
                env.getUserClassLoader(),
                numberOfKeyGroups,
                keyGroupRange,
                env.getExecutionConfig(),
                ttlTimeProvider,
                cancelStreamRegistry);
        playKeyedStateBackend.setPlayStateBackend(this);
        return playKeyedStateBackend;
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(Environment env, String operatorIdentifier, @Nonnull Collection<OperatorStateHandle> stateHandles, CloseableRegistry cancelStreamRegistry) throws Exception {
        return null;
    }

    @Override
    public StateBackend configure(Configuration configuration, ClassLoader classLoader) throws IllegalConfigurationException {
        if (this.getConfiguration() != null) {
            configuration.addAll(this.getConfiguration());
        }
        System.out.println("Refoncig: " + configuration +"\t" + Thread.currentThread().getName());
        return new PlayStateBackend(configuration, classLoader);
    }


    // ===========================================================================
    // Getter and setter
    // ===========================================================================

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    public void setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    public PlayConnection getPlayConnection() {
        return playConnection;
    }

    public void setPlayConnection(PlayConnection playConnection) {
        this.playConnection = playConnection;
    }
}
