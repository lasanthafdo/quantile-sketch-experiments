package LRB.model;

/*
 * As specified in
 * http://www.isys.ucl.ac.be/vldb04/eProceedings/contents/pdf/RS12P1.PDF
 */
public class PositionEvent {

    private final int type;
    private final long time;
    private final int vid;
    private final int spd;
    private final int xWay;
    private final int lane;
    private final int dir;
    private final int seg;
    private final int xPos;
    private final int yPos;

    public PositionEvent(int type, long time, int vid, int spd, int xWay, int lane, int dir, int seg, int xPos, int yPos) {
        this.type = type;
        this.time = time;
        this.vid = vid;
        this.spd = spd;
        this.xWay = xWay;
        this.lane = lane;
        this.dir = dir;
        this.seg = seg;
        this.xPos = xPos;
        this.yPos = yPos;
    }
}
