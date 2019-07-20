package LRB.model;

/*
 * As specified in
 * http://www.isys.ucl.ac.be/vldb04/eProceedings/contents/pdf/RS12P1.PDF
 */
public class AccountBalanceEvent {

    private final int type;
    private final long time;
    private final int vid;
    private final int qid;

    public AccountBalanceEvent(int type, long time, int vid, int qid) {
        this.type = type;
        this.time = time;
        this.vid = vid;
        this.qid = qid;
    }
}
