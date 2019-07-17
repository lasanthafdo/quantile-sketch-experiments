import redis.clients.jedis.Jedis;

/*
 * Generates new benchmark setup
 */
class NewSetupGenerator {

    static final int NUM_CAMPAIGNS = 100;

    private final Jedis jedis;

    NewSetupGenerator(Jedis jedis) {
        this.jedis = jedis;
    }

    void setupWorkload() {
        jedis.flushAll();
        Utils.generateIds(NUM_CAMPAIGNS).forEach(campaign -> jedis.sadd("campaigns", campaign));
    }
}
