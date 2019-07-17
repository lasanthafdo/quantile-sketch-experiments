import redis.clients.jedis.Jedis;

import java.util.HashMap;

class RedisAdCampaignCache {

    private final HashMap<String, String> adToCompaign;
    private final Jedis jedis;

    RedisAdCampaignCache(Jedis jedis) {
        this.jedis = jedis;
        this.adToCompaign = new HashMap<>();
    }

    String execute(String adId) {
        String compaignId = adToCompaign.get(adId);

        if (compaignId == null) {
            compaignId = jedis.get(adId);
            if (compaignId == null) {
                return null;
            } else {
                adToCompaign.put(adId, compaignId);
            }
        }
        return compaignId;
    }
}
