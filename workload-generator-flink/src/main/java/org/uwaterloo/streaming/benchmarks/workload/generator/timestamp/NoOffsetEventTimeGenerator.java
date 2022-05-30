package org.uwaterloo.streaming.benchmarks.workload.generator.timestamp;

public class NoOffsetEventTimeGenerator implements EventTimeGenerator {

    @Override
    public long getEventTimeMillis(long currentTimeMillis) {
        return currentTimeMillis;
    }
}
