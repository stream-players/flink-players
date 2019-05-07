package org.flinkplayers.flink.statebackend.connection;

import org.apache.flink.configuration.Configuration;

public interface PlayConnection {

    void initializeState(Configuration configuration);

}
