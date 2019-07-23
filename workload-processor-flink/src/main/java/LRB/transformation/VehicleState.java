package LRB.transformation;

import LRB.model.LRBEvent;
import LRB.model.VehicleEvent;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;

public class VehicleState extends RichMapFunction<LRBEvent, LRBEvent> {

    private transient ValueState<VehicleEvent> previousPositionReport;

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<VehicleEvent> descriptor =
                new ValueStateDescriptor<>("vehicleState", TypeInformation.of(VehicleEvent.class));
        previousPositionReport = getRuntimeContext().getState(descriptor);
    }

    @Override
    public LRBEvent map(LRBEvent lrbEvent) throws Exception {
        if (lrbEvent.getType() != 0) {
            // Not the type of events we are applying this transformation on
            return lrbEvent;
        }

        VehicleEvent prevEvent = previousPositionReport.value();


    }
}
