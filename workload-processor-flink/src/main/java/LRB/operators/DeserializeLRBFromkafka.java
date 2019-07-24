package LRB.operators;

import LRB.LRBInputRecord;
import org.apache.flink.api.common.functions.MapFunction;

public class DeserializeLRBFromkafka implements MapFunction<String, LRBInputRecord> {

    @Override
    public LRBInputRecord map(String input) {
        return new LRBInputRecord(input);

    }
}