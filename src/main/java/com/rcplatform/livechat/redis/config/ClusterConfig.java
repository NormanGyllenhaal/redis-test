package com.rcplatform.livechat.redis.config;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.resource.DefaultClientResources;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author yang peng
 * @date 2019/4/2215:02
 */
@Configuration
public class ClusterConfig {



    @Value("${redis.cluster}")
    private String clusters;


    @Bean
    public StatefulRedisClusterConnection<String, String> connect(){
        List<RedisURI> uriList = Stream.of(clusters.split(",")).map(s -> {
            String[] split = s.split(":");
            return RedisURI.Builder.redis(split[0], Integer.parseInt(split[1])).withTimeout(Duration.ofSeconds(5)).build();
        }).collect(Collectors.toList());
        RedisClusterClient client = RedisClusterClient.create(DefaultClientResources.create(), uriList);
        ClusterTopologyRefreshOptions topologyRefreshOptions = ClusterTopologyRefreshOptions.builder()
                .enablePeriodicRefresh(Duration.ofSeconds(10))
                .enableAllAdaptiveRefreshTriggers()
                .build();
        client.setOptions(ClusterClientOptions.builder()
                .topologyRefreshOptions(topologyRefreshOptions)
                .build());
        return client.connect();
    }
}
