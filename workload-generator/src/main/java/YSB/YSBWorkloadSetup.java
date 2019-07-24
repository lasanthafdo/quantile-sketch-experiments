package YSB;

import redis.clients.jedis.Jedis;

import static YSB.YSBConstants.NUM_CAMPAIGNS;

/**
 * Sets up a YSB workload setup for an experiment
 */
public class YSBWorkloadSetup implements Runnable {

    /* Jedis instance */
    private final Jedis jedis;

    public YSBWorkloadSetup(Jedis jedis) {
        this.jedis = jedis;
    }

    @Override
    public void run() {
        jedis.flushAll();
        YSBUtils.generateIds(NUM_CAMPAIGNS).forEach(campaign -> jedis.sadd("campaigns", campaign));
    }
}
