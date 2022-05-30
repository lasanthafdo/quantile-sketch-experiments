package eventtime.generator;

import org.apache.commons.math3.distribution.ExponentialDistribution;

public class ExponentialOffsetEventTimeGenerator implements EventTimeGenerator {
    ExponentialDistribution eD = new ExponentialDistribution(150);

    public ExponentialOffsetEventTimeGenerator() {
        System.out.println("NetworkDelayExponential Mean:" + eD.getMean());
    }

    @Override
    public long getEventTimeMillis(long currentTimeMillis) {
        // Network Delay
        int sampled_value = (int) eD.sample();
        return currentTimeMillis - sampled_value;
    }
}
