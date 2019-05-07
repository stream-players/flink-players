package org.flinkplayers.flink.statebackend.examples;

import org.apache.flink.configuration.Configuration;
import org.flinkplayers.flink.statebackend.connection.PlayConnection;

public class RedisNodeConnection implements PlayConnection,java.io.Serializable {
    public RedisNodeConnection(String hostport) {

    }

    @Override
    public void initializeState(Configuration configuration) {

    }
}
