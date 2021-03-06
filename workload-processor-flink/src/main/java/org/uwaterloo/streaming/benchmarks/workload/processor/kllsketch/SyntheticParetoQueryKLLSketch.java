package org.uwaterloo.streaming.benchmarks.workload.processor.kllsketch;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import org.apache.flink.shaded.guava18.com.google.common.base.Preconditions;

import org.apache.datasketches.kll.KllFloatsSketch;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.ArrayList;

public class SyntheticParetoQueryKLLSketch implements Runnable {

    /* The Kafka topic the source operators are pulling the results from */
    private final String KAFKA_PREFIX_TOPIC = "synp-events";
    /* The Job Parameters */
    //This class provides simple utility methods for reading and parsing program
    // arguments from different sources.
    // Only single value parameter could be supported in args.
    private final ParameterTool setupParams;
    /* The num of queries */
    private final int numQueries;
    /* The window size */
    private final int windowSize;

    protected static final Logger LOG = LoggerFactory.getLogger(SyntheticParetoQueryKLLSketch.class);


    public SyntheticParetoQueryKLLSketch(
        ParameterTool setupParams, int numQueries, int windowSize) {
        this.setupParams = setupParams;
        this.numQueries = numQueries;
        this.windowSize = windowSize;
    }


    @Override
    public void run() {
        // Setup Flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(setupParams);
        System.out.println("Running Synthetic Pareto Query");
        LOG.info("Running Synthetic Pareto Query");

        // Add queries
        addQuery(env);

        // Execute queries
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void addQuery(StreamExecutionEnvironment env) {

        WatermarkStrategy<String> wt = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofMillis(0))
            .withTimestampAssigner((event, timestamp) -> Long.parseLong(new JSONObject(event).getString("event_time")));
        DataStream<String> messageStream = env.addSource(
                // Every source is a Kafka Consumer
                new FlinkKafkaConsumer<>(
                    // Different topics for different query queryInstances
                    KAFKA_PREFIX_TOPIC,
                    new SimpleStringSchema(),
                    setupParams.getProperties()).assignTimestampsAndWatermarks(wt).setStartFromEarliest())
            .name("Source").setParallelism(1);
        // Chain all operators before a watermark is emitted.
        //.disableChaining();

        messageStream
            // Parse the JSON string from Kafka as an ad
            .map(new DeserializeMessageFromKafka())
            .name("DeserializeInput ")
            .disableChaining()
            .name("project ")
            //.keyBy(0)
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
            .aggregate(new WindowAdsAggregatorKLLSketch(windowSize))
            .name("DeserializeInput ")
            .name("Window")
            .writeAsText("results-synp-kll.txt", FileSystem.WriteMode.OVERWRITE);
    }


    private static class DeserializeMessageFromKafka implements
        MapFunction<String,
            Tuple3<String, String, String>> {

        @Override
        public Tuple3<String, String, String> map(String input) {
            JSONObject obj = new JSONObject(input);
            return new Tuple3<>(
                obj.getString("pareto_value"), // 0
                obj.getString("event_time"), // 1
                String.valueOf(System.currentTimeMillis()) // 2 ingestion_time
            );
        }
    }

    public static Double percentile(ArrayList<Double> sort_values, double percentile) {
        Preconditions.checkArgument(percentile > 0);
        Preconditions.checkArgument(percentile < 100);
        int index = (int) Math.ceil(percentile / 100.0 * sort_values.size());
        return sort_values.get(index - 1);
    }

    private static class WindowAdsAggregatorKLLSketch implements AggregateFunction<
        Tuple3<String, String, String>,
        Tuple2<Long, KllFloatsSketch>,
        Tuple2<Long, ArrayList<Double>>> {

        double[] percentiles = {.01, .05, .25, .50, .75, .90, .95, .98, .99};
        int windowSizeAgg;

        public WindowAdsAggregatorKLLSketch(int windowSize) {
            this.windowSizeAgg = windowSize * 1000; // convert to milliseconds
        }

        @Override
        public Tuple2<Long, KllFloatsSketch> createAccumulator() {
            return new Tuple2<>(0L, new KllFloatsSketch(350));
        }

        @Override
        public Tuple2<Long, KllFloatsSketch> add(Tuple3<String, String, String> value,
                                                 Tuple2<Long, KllFloatsSketch> accumulator) {
            accumulator.f1.update(Float.parseFloat(value.f0)); // f0 is pareto
            accumulator.f0 = Long.parseLong(value.f1) / windowSizeAgg;
            return accumulator;
        }

        @Override
        public Tuple2<Long, KllFloatsSketch> merge(Tuple2<Long, KllFloatsSketch> acc0,
                                                   Tuple2<Long, KllFloatsSketch> acc1) {
            acc0.f1.merge(acc1.f1);
            return acc0;
        }

        @Override
        public Tuple2<Long, ArrayList<Double>> getResult(Tuple2<Long, KllFloatsSketch> accumulator) {
            long start = System.nanoTime();
            Tuple2<Long, ArrayList<Double>> ret_tuple = new Tuple2<>();
            ret_tuple.f0 = accumulator.f0;
            ret_tuple.f1 = new ArrayList<>();

            System.out.println("KLL Struct");
            System.out.println(accumulator.f1);

            float[] results = accumulator.f1.getQuantiles(percentiles);

            for (float d : results) {
                ret_tuple.f1.add(round(d, 4));
            }

            long end = System.nanoTime();
            long elapsed_time = end - start;
            System.out.println("-------------------------------------------------------------");
            System.out.println("Retrieving result took " + elapsed_time / 1000 + "microseconds");
            LOG.info("Retrieving result took " + elapsed_time / 1000 + "microseconds");
            ret_tuple.f1.add((double) elapsed_time);
            double numRetained = (double) accumulator.f1.getNumRetained();
            ret_tuple.f1.add(numRetained);
            System.out.println(accumulator.f1.toString());
            System.out.println("-------------------------------------------------------------");
            return ret_tuple;
        }

        private double round(double value, int places) {
            if (places < 0) {
                throw new IllegalArgumentException();
            }

            BigDecimal bd = new BigDecimal(Double.toString(value));
            bd = bd.setScale(places, RoundingMode.HALF_UP);
            return bd.doubleValue();
        }

    }
}
