package LRB.model;

import java.io.Serializable;

public class LRBEvent implements Serializable {

    private final int type;
    private final long time;

    public LRBEvent(int type, long time) {
        this.type = type;
        this.time = time;
    }
}
