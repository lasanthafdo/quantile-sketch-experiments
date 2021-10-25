package Moments;

import com.github.stanfordfuturedata.momentsketch.MomentSolver;
import com.github.stanfordfuturedata.momentsketch.MomentStruct;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.shaded.guava18.com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.ArrayList;

import static java.lang.Double.parseDouble;


import org.apache.flink.api.java.tuple.*;

import java.util.concurrent.ThreadLocalRandom;

public class SyntheticQuery implements Runnable {

    /* The Kafka topic the source operators are pulling the results from */
    private final String KAFKA_PREFIX_TOPIC = "syn-events";
    /* The Job Parameters */
    //This class provides simple utility methods for reading and parsing program
    // arguments from different sources.
    // Only single value parameter could be supported in args.
    private final ParameterTool setupParams;
    /* The scheduler policy */
//    private final StreamTaskSchedulerPolicy schedulerPolicy;
    /* The num of queries */
    private final int numQueries;
    /* The window size */
    private final int windowSize;
    private JSONObject js;

    protected static final Logger LOG = LoggerFactory.getLogger(SyntheticQuery.class);


    public SyntheticQuery(
            ParameterTool setupParams, int numQueries, int windowSize) {
        this.setupParams = setupParams;
//        this.schedulerPolicy = schedulerPolicy;
        this.numQueries = numQueries;
        this.windowSize = windowSize;
    }


    @Override
    public void run() {
        // Setup Flink
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.setStreamTaskSchedulerPolicy(schedulerPolicy);
        env.getConfig().setGlobalJobParameters(setupParams);
        System.out.println("Running Synthetic Query");
        LOG.info("Running Synthetic Query");

        // Add queries
        addQuery(env);

        // Execute queries
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void addQuery(StreamExecutionEnvironment env){

        WatermarkStrategy<String> wt = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofMillis(0))
                .withTimestampAssigner((event, timestamp) -> Long.parseLong(new JSONObject(event).getString("event_time")));
        //Long.parseLong(new JSONObject(event).getString("event_time")
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
                .map(new SyntheticQuery.DeserializeFromkafka())
                .name("DeserializeInput ")
                .disableChaining()
                .name("project ")
                //.keyBy(0)
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                .aggregate(new SyntheticQuery.WindowAdsAggregatorMSketch())
                .name("DeserializeInput ")
                .name("Window")
                .writeAsText("results-synthetic-moments.txt", FileSystem.WriteMode.OVERWRITE);
        // sink function
        //.addSink(new PrintCampaignAdClicks())
        //.name("Sink(" + queryInstance + ")");
    }


    private class DeserializeFromkafka implements
            MapFunction<String,
                    Tuple5<String, String, String, String, String>> {

        @Override
        public Tuple5<String, String, String, String, String> map(String input) {
            JSONObject obj = new JSONObject(input);
            Boolean fake = false;
            if (obj.has("fake")){
                fake = true;
            }
            return new Tuple5<>(
                    obj.getString("pareto_value"), // 0
                    obj.getString("uniform_value"), // 1
                    obj.getString("normal_value"), // 2
                    obj.getString("event_time"), // 3
                    String.valueOf(System.currentTimeMillis()) // 4 ingestion_time
            );
        }
    }

    public static Double percentile(ArrayList<Double> sort_values, double percentile) {
        Preconditions.checkArgument(percentile > 0);
        Preconditions.checkArgument(percentile < 100);
        int index = (int) Math.ceil(percentile / 100.0 * sort_values.size());
        return sort_values.get(index-1);
    }

    private class WindowAdsAggregatorMSketch implements AggregateFunction<
            Tuple5<String, String, String, String, String>,
            Tuple2<Long, MomentStruct>,
            Tuple2<Long, ArrayList<Double>>> {

        double[] percentiles = {.01, .05, .25, .50, .75, .90, .95, .98};

        @Override
        public Tuple2<Long, MomentStruct> createAccumulator() {
            MomentStruct ms = new MomentStruct(15);
            Tuple2<Long, MomentStruct> ms_tuple = new Tuple2<>(0L, new MomentStruct(15));
            ms_tuple.f1 = ms;

            return ms_tuple;
        }

        @Override
        public Tuple2<Long, MomentStruct> add(Tuple5<String, String, String, String, String> value,
                                              Tuple2<Long, MomentStruct> accumulator) {
            accumulator.f1.add(parseDouble(value.f0)); // f0 is pareto, f1 is uniform, f2 is normal
            if (ThreadLocalRandom.current().nextInt(0,100) > 90){
                System.out.println("Adding-value " + Double.toString(parseDouble(value.f0)));
            }
            int WINDOW_SIZE = 6000; // in milliseconds
            accumulator.f0 = Long.parseLong(value.f3)/WINDOW_SIZE;
            return accumulator;
        }

        @Override
        public Tuple2<Long, MomentStruct> merge(Tuple2<Long, MomentStruct> acc0,
                                                Tuple2<Long, MomentStruct> acc1) {
            acc0.f1.merge(acc1.f1);
            return acc0;
        }

        @Override
        public Tuple2<Long, ArrayList<Double>> getResult(Tuple2<Long, MomentStruct> accumulator) {
            long start = System.currentTimeMillis();
            Tuple2<Long, ArrayList<Double>> ret_tuple = new Tuple2<>();
            ret_tuple.f0 = accumulator.f0;
            ret_tuple.f1 = new ArrayList<>();

            System.out.println("Moments Struct");
            System.out.println(accumulator.f1);

            MomentSolver ms = new MomentSolver(accumulator.f1);
            ms.setGridSize(1024);
            ms.solve();

            double[] results = ms.getQuantiles(percentiles);

            for(double d : results) ret_tuple.f1.add(round(d, 2));

            long end = System.currentTimeMillis();
            long elapsed_time = end - start;
            System.out.println("Retrieving result took " + elapsed_time + "milliseconds");
            LOG.info("Retrieving result took " + elapsed_time + "milliseconds");
            ret_tuple.f1.add((double) elapsed_time);
            return ret_tuple;
        }

        private double round(double value, int places) {
            if (places < 0) throw new IllegalArgumentException();

            BigDecimal bd = new BigDecimal(Double.toString(value));
            bd = bd.setScale(places, RoundingMode.HALF_UP);
            return bd.doubleValue();
        }

    }
}
