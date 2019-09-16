package LRB;

import LRB.operators.DeserializeLRBFromkafka;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.runtime.tasks.scheduler.StreamTaskSchedulerPolicy;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Iterator;

public class LinearRoadQuery implements Runnable {

    /* The Kafka topic the source operators are pulling the results from */
    private final String KAFKA_PREFIX_TOPIC = "lrb-events";
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
        DataStream<LRBInputRecord> sourceStream = env.addSource(
                // Every source is a Kafka Consumer
                new FlinkKafkaConsumer<>(
                        // Different topics for different query queryInstances
                        KAFKA_PREFIX_TOPIC + "-" + queryInstance,
                        new SimpleStringSchema(),
                        setupParams.getProperties()))
                .name("Source (" + queryInstance + ")")
                // Chain all operators before a watermark is emitted.
                .startNewChain()
                .map(new DeserializeLRBFromkafka());


        DataStream<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> intermediate =
                sourceStream.filter(new FilterFunction<LRBInputRecord>() {
                    @Override
                    public boolean filter(LRBInputRecord value) throws Exception {
                        return value.getM_iType() == 0;
                    }
                }).assignTimestampsAndWatermarks(new LRBWatermarkAndTimeStampAssigner())
                        .map(new MapFunction<LRBInputRecord, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
                            @Override
                            public Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> map(LRBInputRecord in) throws Exception {
                                return new Tuple6<>(in.getM_iVid(), in.getM_iSeg(),
                                        in.getM_iSpeed(), in.getM_iPos(),
                                        in.getM_iTime(), in.getM_iXway());
                            }
                        });


        // 0:vid, 1:seg, 2:speed, 3:pos, 4:time, 5:xway

        // speed monitor - segment, xway, avg speed
        DataStream<Tuple3<Integer, Integer, Double>> speedStreams = intermediate
                .keyBy(1, 5) // key by xway and seg
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .apply(new RichWindowFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>,
                        Tuple3<Integer, Integer, Double>, Tuple, TimeWindow>() {


                    @Override
                    public void apply(Tuple tuple,
                                      TimeWindow window,
                                      Iterable<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> elements,
                                      Collector<Tuple3<Integer, Integer, Double>> out) throws Exception {

                        Iterator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> it = elements.iterator();

                        double speed = 0.0;
                        long counter = 0L;

                        while (it.hasNext()) {
                            Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> curr = it.next();
                            speed += curr.f2;
                            counter++;
                        }

                        Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> head = elements.iterator().next();

                        out.collect(new Tuple3<>(head.f1, head.f5, speed / counter));
                    }
                }).name("SpeedWindow (" + queryInstance + ")");

        // vehicle stream - seg, xway, vcounter
        DataStream<Tuple3<Integer, Integer, Integer>> vehiclesStream = intermediate
                .keyBy(1, 5)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .apply(new WindowFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>,
                        Tuple3<Integer, Integer, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple,
                                      TimeWindow window,
                                      Iterable<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> elements,
                                      Collector<Tuple3<Integer, Integer, Integer>> out) throws Exception {

                        Iterator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> it = elements.iterator();

                        HashSet<Integer> counter = new HashSet<>();

                        while (it.hasNext()) {
                            Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> curr = it.next();
                            counter.add(curr.f0);
                        }

                        Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> head = elements.iterator().next();

                        out.collect(new Tuple3<>(head.f1, head.f5, counter.size()));

                    }
                }).name("Vehicle Window (" + queryInstance + ")" );

        // accident counter - vid, seg, xway, pos, time
        DataStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> accidentsStream = intermediate
                .keyBy(1) // segment
                .window(SlidingEventTimeWindows.of(Time.seconds(3), Time.seconds(2)))
                .process(new ProcessWindowFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>,
                        Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple, TimeWindow>() {

                    transient private MapState<Integer, Tuple3<Integer, Integer, Long>> stopped;

                    @Override
                    public void process(Tuple tuple,
                                        Context context,
                                        Iterable<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> elements,
                                        Collector<Tuple5<Integer, Integer, Integer, Integer, Integer>> out) throws Exception {
                        Iterator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> it = elements.iterator();
                        while (it.hasNext()) {

                            Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> curr = it.next();

                            if (curr.f2 == 0) {

                                int vid = curr.f0;
                                int currPos = curr.f3;

                                if (!stopped.contains(vid)) {
                                    stopped.put(vid, new Tuple3<>(1, currPos, context.currentProcessingTime()));
                                } else {
                                    int oldPos = stopped.get(vid).f1;
                                    int cnt = stopped.get(vid).f0;
                                    long timestamp = stopped.get(vid).f2;
                                    long now = context.currentProcessingTime();
                                    if (currPos != oldPos || (now - timestamp) < (3 * 1000)) {
                                        // reset counter
                                        stopped.put(vid, new Tuple3<>(1, currPos, now));
                                    } else if (cnt == 4) {
                                        // possible accident
                                        out.collect(new Tuple5<>(vid, curr.f1, curr.f5, currPos, curr.f4));
                                    } else {
                                        // increase the counter
                                        stopped.put(vid, new Tuple3<>(cnt + 1, currPos, timestamp));
                                    }
                                }
                            }
                        }
                    }


                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        stopped = getRuntimeContext().getMapState(
                                new MapStateDescriptor<>("stopped",
                                        TypeInformation.of(new TypeHint<Integer>() {
                                        }),
                                        TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, Long>>() {
                                        })));
                    }

                }).name("Accident Window (" + queryInstance + ")");


        DataStream<Tuple4<Integer, Integer, Integer, Double>> tollsStream = vehiclesStream
                .join(speedStreams)
                .where(new KeySelector<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> getKey(Tuple3<Integer, Integer, Integer> value) throws Exception {
                        return new Tuple2<>(value.f0, value.f1);
                    }
                })
                .equalTo(new KeySelector<Tuple3<Integer, Integer, Double>, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> getKey(Tuple3<Integer, Integer, Double> value) throws Exception {
                        return new Tuple2<>(value.f0, value.f1);
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .apply(new JoinFunction<Tuple3<Integer, Integer, Integer>,
                        Tuple3<Integer, Integer, Double>,
                        Tuple4<Integer, Integer, Integer, Double>>() {
                    @Override
                    public Tuple4<Integer, Integer, Integer, Double> join(Tuple3<Integer, Integer, Integer> first,
                                                                          Tuple3<Integer, Integer, Double> second) throws Exception {


                        if (first.f2 > 50 && second.f2 < 40.0) {
                            return new Tuple4<>(first.f0, first.f1, 2 * (first.f2 - 50) ^ 2, second.f2);
                        } else {
                            return new Tuple4<>(first.f0, first.f1, 0, second.f2);
                        }

                    }
                })
                .coGroup(accidentsStream)
                .where(new KeySelector<Tuple4<Integer, Integer, Integer, Double>, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> getKey(Tuple4<Integer, Integer, Integer, Double> value) throws Exception {
                        return new Tuple2<>(value.f0, value.f1);
                    }
                })
                .equalTo(new KeySelector<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> getKey(Tuple5<Integer, Integer, Integer, Integer, Integer> value) throws Exception {
                        return new Tuple2<>(value.f1, value.f2);
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .apply(new CoGroupFunction<Tuple4<Integer, Integer, Integer, Double>,
                        Tuple5<Integer, Integer, Integer, Integer, Integer>,
                        Tuple4<Integer, Integer, Integer, Double>>() {
                    @Override
                    public void coGroup(Iterable<Tuple4<Integer, Integer, Integer, Double>> first,
                                        Iterable<Tuple5<Integer, Integer, Integer, Integer, Integer>> second,
                                        Collector<Tuple4<Integer, Integer, Integer, Double>> out) throws Exception {

                        Iterator<Tuple5<Integer, Integer, Integer, Integer, Integer>> it = second.iterator();
                        Tuple4<Integer, Integer, Integer, Double> head = first.iterator().next();

                        if (it.hasNext()) {
                            out.collect(head);
                        } else {
                            out.collect(new Tuple4<>(head.f0, head.f1, 0, head.f3));
                        }

                    }
                });


        tollsStream.addSink(new SinkFunction<Tuple4<Integer, Integer, Integer, Double>>() {
            @Override
            public void invoke(Tuple4<Integer, Integer, Integer, Double> value, Context contet) throws Exception {
                // Do nothing
            }
        }).name("Sink Function");
    }

    private class LRBWatermarkAndTimeStampAssigner implements AssignerWithPeriodicWatermarks<LRBInputRecord> {

        int eventsUntilWatermark = 1000;
        Watermark watermark;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return watermark;
        }

        @Override
        public long extractTimestamp(LRBInputRecord lrbInputRecord, long l) {
            long timestamp = lrbInputRecord.getM_iTime() * 1000;
            if (eventsUntilWatermark == 0) {
                watermark = new Watermark(timestamp - 2000);
                eventsUntilWatermark = 1000;
            }
            eventsUntilWatermark--;
            return timestamp;
        }
    }
}
