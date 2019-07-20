package LRB;

import LRB.model.AccountBalanceEvent;
import LRB.model.DailyExpenditureEvent;
import LRB.model.LRBEvent;
import LRB.model.PositionEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.runtime.tasks.scheduler.StreamTaskSchedulerPolicy;
import org.json.JSONObject;

import static LRB.LRBConstants.*;

public class LinearRoadQuery implements Runnable {

    /* The Kafka topic the source operators are pulling the results from */
    private final String KAFKA_PREFIX_TOPIC = "lrb";
    /* The Job Parameters */
    private final ParameterTool setupParams;
    /* The scheduler policy */
    private final StreamTaskSchedulerPolicy schedulerPolicy;
    /* The num of queries */
    private final int numQueries;

    public LinearRoadQuery(
            ParameterTool setupParams, StreamTaskSchedulerPolicy schedulerPolicy, int numQueries) {
        this.setupParams = setupParams;
        this.schedulerPolicy = schedulerPolicy;
        this.numQueries = numQueries;
    }

    public void run() {
        // Setup Flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setStreamTaskSchedulerPolicy(schedulerPolicy);
        env.getConfig().setGlobalJobParameters(setupParams);

        // Add queries
        for (int i = 1; i <= numQueries; i++) {
            addQuery(env, i);
        }

        // Execute queries
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void addQuery(StreamExecutionEnvironment env, int queryInstance) {
        DataStream<String> sourceStream = env.addSource(
                // Every source is a Kafka Consumer
                new FlinkKafkaConsumer<>(
                        // Different topics for different query queryInstances
                        KAFKA_PREFIX_TOPIC + "-" + queryInstance,
                        new SimpleStringSchema(),
                        setupParams.getProperties()))
                .name("Source (" + queryInstance + ")");

        sourceStream
                // Deserialize event type
                .map(new DeserializeLRBEventsFromKafka());
                // Determine Accidents
    }

    private class DeserializeLRBEventsFromKafka implements MapFunction<String, LRBEvent> {

        @Override
        public LRBEvent map(String input) {
            JSONObject obj = new JSONObject(input);
            int type = obj.getInt("type");

            if (type == TYPE_POSITION_REPORT) {
                return new PositionEvent(type,
                        obj.getLong("time"),
                        obj.getInt("vid"),
                        obj.getInt("spd"),
                        obj.getInt("xWay"),
                        obj.getInt("lane"),
                        obj.getInt("dir"),
                        obj.getInt("seg"),
                        obj.getInt("xPos"),
                        obj.getInt("yPos"));
            } else if (type == TYPE_ACCOUNT_BALANCE_REQUEST) {
                return new AccountBalanceEvent(type,
                        obj.getLong("time"),
                        obj.getInt("vid"),
                        obj.getInt("qid"));
            } else if (type == TYPE_DAILY_EXPENDITURE_REQUEST) {
                return new DailyExpenditureEvent(type,
                        obj.getLong("time"),
                        obj.getInt("vid"),
                        obj.getInt("xway"),
                        obj.getInt("qid"),
                        obj.getInt("day"));
            } else {
                // TODO(oibfarhat): Support all type of queries.
                throw new UnsupportedOperationException("Unsupported type of queries");
            }
        }
    }
}
