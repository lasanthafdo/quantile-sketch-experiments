package LRB.model;

/*
 * As specified in
 * http://www.isys.ucl.ac.be/vldb04/eProceedings/contents/pdf/RS12P1.PDF
 */
public class AccountBalanceEvent extends LRBEvent {
    
    private final int vid;
    private final int qid;

    public AccountBalanceEvent(int type, long time, int vid, int qid) {
        super(type, time);
        this.vid = vid;
        this.qid = qid;
    }
}
