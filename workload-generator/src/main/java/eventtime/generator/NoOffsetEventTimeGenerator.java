package eventtime.generator;

public class NoOffsetEventTimeGenerator implements EventTimeGenerator {

    @Override
    public long getEventTimeMillis(long currentTimeMillis) {
        return currentTimeMillis;
    }
}
