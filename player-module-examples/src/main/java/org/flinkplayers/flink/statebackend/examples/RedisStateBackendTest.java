package org.flinkplayers.flink.statebackend.examples;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.flinkplayers.flink.statebackend.PlayStateBackend;

import java.io.Serializable;

/**
 * Unit test for simple App.
 */
public class RedisStateBackendTest implements java.io.Serializable{
    /**
     * Rigorous Test :-)
     */
    public static void main(String[] args) throws Exception {
        RedisNodeConnection connection = new RedisNodeConnection("localhost:6379");
        PlayStateBackend redisStateBackend = new PlayStateBackend();
        redisStateBackend.setPlayConnection(connection);
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironment();
        environment.setRestartStrategy(RestartStrategies.noRestart());
        environment
                .enableCheckpointing(10000)
                .setStateBackend(redisStateBackend)
                .setParallelism(1)
                .fromElements(
                        "just for me and for me for ever and then sk loasd",
                        "just for me and for me for ever and then sk loasd ever and then sk loasd",
                        "just for me and for me for ever and for me for ever and then sk loasd",
                        "just for me and for me just for me for ever for ever and then sk loasd"
                ).name("operator-elements-source").uid("operator-elements-source")
                .flatMap(new MyFlatMapFunction()).name("operator-flatmap-01").uid("operator-flatmap-01")
                .keyBy(0)
                .map(new MyStateMapFunction()).name("operator-map-with-state-01").uid("operator-map-with-state-01")
                .print();
        environment.execute();
    }

    public static class MyFlatMapFunction implements FlatMapFunction<String, Tuple1<String>>,java.io.Serializable {
        @Override
        public void flatMap(String value, Collector<Tuple1<String>> out) throws Exception {
            Thread.sleep(8000);
            String[] kes = value.split(" ");
            for (String ke : kes) {
                out.collect(new Tuple1<>(ke));
            }
        }
    }

    public static class KeyCount implements Serializable {
        private String key;
        private int count;

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "KeyCount{" +
                    "key='" + key + '\'' +
                    ", count=" + count +
                    '}';
        }
    }

    public static class MyStateMapFunction extends RichMapFunction<Tuple1<String>, KeyCount> implements CheckpointedFunction,java.io.Serializable {

        ValueState<KeyCount> state;

        @Override
        public KeyCount map(Tuple1<String> value) throws Exception {
            KeyCount keyCount = state.value();
            if (keyCount == null) {
                keyCount = new KeyCount();
                keyCount.setKey(value.f0);
            }
            keyCount.setCount(keyCount.getCount() + 1);
            state.update(keyCount);
            return keyCount;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ValueStateDescriptor<KeyCount> valueStateDescriptor = new ValueStateDescriptor<>("dasd", KeyCount.class);
            state = getRuntimeContext().getState(valueStateDescriptor);
        }

        @Override
        public void close() throws Exception {
            super.close();
            state = null;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("Map CheckpointedFunction snapshotState");
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("Map CheckpointedFunction Init initializeState");
        }
    }


}