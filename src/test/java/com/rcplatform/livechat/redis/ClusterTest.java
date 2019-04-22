package com.rcplatform.livechat.redis;

import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author yang peng
 * @date 2019/4/2213:52
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = RedisTestApplication.class)
public class ClusterTest {


    @Resource
    private StatefulRedisClusterConnection<String, String> connect;


    private CountDownLatch countDownLatch = new CountDownLatch(10);


    private ExecutorService ex = Executors.newFixedThreadPool(10);


    private ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();


    private static final Integer SIZE = 2000;

    private Logger logger = LoggerFactory.getLogger(ClusterTest.class);


    private Map<String, Long> totalCount = new ConcurrentHashMap<>();


    private Map<String, Long> totalTime = new ConcurrentHashMap<>();


    private Map<String, Long> time0 = new ConcurrentHashMap<>();


    private Map<String, Long> time1 = new ConcurrentHashMap<>();


    private Map<String, Long> time2 = new ConcurrentHashMap<>();


    private Map<String, Long> time3 = new ConcurrentHashMap<>();


    private Map<String, Long> time5 = new ConcurrentHashMap<>();


    private Map<String, Long> time10 = new ConcurrentHashMap<>();


    @Test
    public void clusterTest() {
        for (int i = 0; i < 16; i++) {
            ex.execute(() -> {
                for (int j = 0; j < 1000000; j++) {
                    logger.info("当前 {}", j);
                    testRedis();
                }
                countDownLatch.countDown();
            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        printResult();
    }


    private void printResult() {
        for (Map.Entry<String, Long> entry : totalCount.entrySet()) {
            logger.info("================begin===================");
            logger.info("{} 总次数 {}", entry.getKey(), entry.getValue());
            logger.info("{} 总时间 {}", entry.getKey(), totalTime.get(entry.getKey()));
            double avgTime = (double) totalTime.get(entry.getKey()) / entry.getValue();
            logger.info("{} 平均时间 {}", entry.getKey(), avgTime);
            logger.info("{} 小于 1ms {} {}", entry.getKey(), time0.get(entry.getKey()), time0.get(entry.getKey()) != null ? (double) time0.get(entry.getKey()) / entry.getValue() : 0);
            logger.info("{} 1ms-2ms {} {}", entry.getKey(), time1.get(entry.getKey()), time1.get(entry.getKey()) != null ? (double) time1.get(entry.getKey()) / entry.getValue() : 0);
            logger.info("{} 2ms-3ms {} {}", entry.getKey(), time2.get(entry.getKey()), time2.get(entry.getKey()) != null ? (double) time2.get(entry.getKey()) / entry.getValue() : 0);
            logger.info("{} 3ms-5ms {} {}", entry.getKey(), time3.get(entry.getKey()), time3.get(entry.getKey()) != null ? (double) time3.get(entry.getKey()) / entry.getValue() : 0);
            logger.info("{} 5ms-10ms {} {}", entry.getKey(), time5.get(entry.getKey()), time5.get(entry.getKey()) != null ? (double) time5.get(entry.getKey()) / entry.getValue() : 0);
            logger.info("{} 10ms以上 {} {}", entry.getKey(), time10.get(entry.getKey()), time10.get(entry.getKey()) != null ? (double) time10.get(entry.getKey()) / entry.getValue() : 0);
            logger.info("================end===================");
        }
    }


    private void testRedis() {
        try {
            Long begin;
            for (int i = 0; i < SIZE; i++) {
                String stringKey = RandomStringUtils.randomAlphabetic(10);
                //string
                String strValue = RandomStringUtils.randomAlphabetic(100);
                begin = System.currentTimeMillis();
                connect.sync().setex(stringKey, TimeUnit.HOURS.toSeconds(1),strValue);
                saveTime(begin, "set");
                begin = System.currentTimeMillis();
                connect.sync().get(stringKey);
                saveTime(begin, "get");
            }
            //hash
            String hashKey = RandomStringUtils.randomAlphabetic(9);
            for (int i = 0; i < SIZE; i++) {
                String field = RandomStringUtils.randomAlphabetic(5);
                String value = String.valueOf(threadLocalRandom.nextInt());
                begin = System.currentTimeMillis();
                connect.sync().hset(hashKey, field,value);
                saveTime(begin, "hset");
                begin = System.currentTimeMillis();
                connect.sync().hget(hashKey, field);
                saveTime(begin, "hget");
            }
            connect.sync().expire(hashKey, TimeUnit.HOURS.toSeconds(1));
            begin = System.currentTimeMillis();
            connect.sync().hgetall(hashKey);
            saveTime(begin, "hgetall");
            //list
            String listKey = RandomStringUtils.random(8);
            for (int i = 0; i < SIZE; i++) {
                String listValue = RandomStringUtils.random(5);
                begin = System.currentTimeMillis();
                connect.sync().lpush(listKey, listValue);
                saveTime(begin, "lpush");
            }
            connect.sync().expire(listKey, TimeUnit.HOURS.toSeconds(1));
            begin = System.currentTimeMillis();
            connect.sync().lrange(listKey, 0, -1);
            saveTime(begin,"lrange");
            //set
            String setKey = RandomStringUtils.random(6);
            for (int i = 0; i < SIZE; i++) {
                String s = String.valueOf(threadLocalRandom.nextInt());
                begin = System.currentTimeMillis();
                connect.sync().sadd(setKey, s);
                saveTime(begin,"sadd");
            }
            connect.sync().expire(setKey, TimeUnit.HOURS.toSeconds(1));
            begin = System.currentTimeMillis();
            connect.sync().smembers(setKey);
            saveTime(begin,"smembers");
            //zset
            String zsetKey = RandomStringUtils.random(7);
            for (int i = 0; i < SIZE; i++) {
                String s = String.valueOf(threadLocalRandom.nextInt());
                begin = System.currentTimeMillis();
                connect.sync().zadd(zsetKey, (double) System.currentTimeMillis(),s);
                saveTime(begin,"zadd");
            }
            connect.sync().expire(zsetKey, TimeUnit.HOURS.toSeconds(1));
            begin = System.currentTimeMillis();
            connect.sync().zrange(zsetKey, 0, -1);
            saveTime(begin,"zrange");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void saveTime(Long beginTime, String command) {
        Long diff = System.currentTimeMillis() - beginTime;
        totalCount.merge(command, 1L, (old, newValue) -> old + newValue);
        totalTime.merge(command, diff, (old, newValue) -> old + newValue);
        if (diff < 1) {
            time0.merge(command, 1L, (old, newValue) -> old + newValue);
        } else if (diff < 2) {
            time1.merge(command, 1L, (old, newValue) -> old + newValue);
        } else if (diff < 3) {
            time2.merge(command, 1L, (old, newValue) -> old + newValue);
        } else if (diff < 5) {
            time3.merge(command, 1L, (old, newValue) -> old + newValue);
        } else if (diff < 10) {
            time5.merge(command, 1L, (old, newValue) -> old + newValue);
        } else {
            time10.merge(command, 1L, (old, newValue) -> old + newValue);
        }
    }
}
