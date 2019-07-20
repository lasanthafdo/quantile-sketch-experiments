package LRB.model;

/*
 * As specified in
 * http://www.isys.ucl.ac.be/vldb04/eProceedings/contents/pdf/RS12P1.PDF
 */
public class TravelTimeEvent {

    private final int type;
    private final long time;
    private final int vid;
    private final int xway;
    private final int qid;
    private final int sinit;
    private final int send;
    private final int dow;
    private final int tod;


    public TravelTimeEvent(int type, long time, int vid, int xway, int qid, int sinit, int send, int dow, int tod) {
        this.type = type;
        this.time = time;
        this.vid = vid;
        this.xway = xway;
        this.qid = qid;
        this.sinit = sinit;
        this.send = send;
        this.dow = dow;
        this.tod = tod;
    }
}
