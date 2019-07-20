package LRB.model;

/*
 * As specified in
 * http://www.isys.ucl.ac.be/vldb04/eProceedings/contents/pdf/RS12P1.PDF
 */
public class DailyExpenditureEvent extends LRBEvent {

    private final int vid;
    private final int xway;
    private final int qid;
    private final int day;

    public DailyExpenditureEvent(int type, long time, int vid, int xway, int qid, int day) {
        super(type, time);
        this.vid = vid;
        this.xway = xway;
        this.qid = qid;
        this.day = day;
    }
}
