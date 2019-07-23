package LRB.model;

public class VehicleEvent extends LRBEvent {

    private final PositionEvent positionEvent;
    private final int samePositionCounter;
    private final boolean isCrossing;
    private final boolean isStopped;

    public VehicleEvent(PositionEvent positionEvent, int samePositionCounter, boolean isCrossing, boolean isStopped) {
        super(positionEvent.getType(), positionEvent.getTime());
        this.positionEvent = positionEvent;
        this.samePositionCounter = samePositionCounter;
        this.isCrossing = isCrossing;
        this.isStopped = isStopped;
    }
}
