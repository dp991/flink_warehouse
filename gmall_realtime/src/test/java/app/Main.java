package app;


import org.example.utils.RedisUtil;
import org.liwei_data.util.CommonUtil;
import redis.clients.jedis.Jedis;

public class Main {
    public static void main(String[] args) {
//        Jedis jedis = RedisUtil.getJedis();
//        System.out.println(jedis.get("Flink_长沙_洞口金龙山_竹市镇金龙村"));

        String address = "正常情况下,单程50分,间隔10-15分";
        int start = address.indexOf("单程");
        int end = address.indexOf("分");
        System.out.println(address.substring(start+2,end));
    }
}

