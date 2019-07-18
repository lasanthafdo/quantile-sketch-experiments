import redis.clients.jedis.Jedis;

import java.util.HashMap;

import static java.util.UUID.randomUUID;

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
                compaignId =  randomUUID().toString();
                adToCompaign.put(adId, compaignId);
            }
        }
        adToCompaign.put(adId, compaignId);
        return compaignId;
    }
}
