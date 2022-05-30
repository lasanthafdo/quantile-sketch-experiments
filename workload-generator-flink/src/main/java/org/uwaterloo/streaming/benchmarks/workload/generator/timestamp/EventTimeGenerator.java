package org.uwaterloo.streaming.benchmarks.workload.generator.timestamp;

/**
 * Generates an event time that is negatively offset by a sampled value from a specified distribution
 */
public interface EventTimeGenerator {
    long getEventTimeMillis(long currentTimeMillis);
}
