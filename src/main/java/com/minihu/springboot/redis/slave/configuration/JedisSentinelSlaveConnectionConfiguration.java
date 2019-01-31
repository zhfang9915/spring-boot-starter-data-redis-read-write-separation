/**
 * Copyright (C), 2016-2019, 上海理想信息产业(集团)有限公司
 * FileName: ConnectionPoolAutoConfiguration
 *
 * @author: zhfang
 * Date: 2019/1/30 14:21
 * Description:
 */
package com.minihu.springboot.redis.slave.configuration;

import com.minihu.springboot.redis.slave.JedisSentinelSlaveConnectionFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.data.redis.JedisClientConfigurationBuilderCustomizer;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnection;
import org.springframework.util.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

/**
 * @CalssName: ConnectionPoolAutoConfiguration
 * @Descrption:
 * @Author: zhfang
 * @Date: 2019/1/30 14:21
 **/
@Configuration
@EnableConfigurationProperties(RedisProperties.class)
@ConditionalOnClass({ GenericObjectPool.class, JedisConnection.class, Jedis.class })
public class JedisSentinelSlaveConnectionConfiguration extends RedisSentinelSlaveConnectionConfiguration {

    private final RedisProperties properties;

    private final List<JedisClientConfigurationBuilderCustomizer> builderCustomizers;

    JedisSentinelSlaveConnectionConfiguration(RedisProperties properties,
                                              ObjectProvider<RedisSentinelConfiguration> sentinelConfiguration,
                                              ObjectProvider<RedisClusterConfiguration> clusterConfiguration,
                                              ObjectProvider<List<JedisClientConfigurationBuilderCustomizer>> builderCustomizers) {
        super(properties, sentinelConfiguration, clusterConfiguration);
        this.properties = properties;
        this.builderCustomizers = builderCustomizers
                .getIfAvailable(Collections::emptyList);
    }

    @Bean
    @ConditionalOnMissingBean
    public JedisSentinelSlaveConnectionFactory redisSlaveConnectionFactory() {
        return createJedisSlaveConnectionFactory();
    }

    private JedisSentinelSlaveConnectionFactory createJedisSlaveConnectionFactory() {
        JedisClientConfiguration clientConfiguration = getJedisSlaveClientConfiguration();
        return new JedisSentinelSlaveConnectionFactory(getSentinelConfig(), clientConfiguration);
    }

    private JedisClientConfiguration getJedisSlaveClientConfiguration() {
        JedisClientConfiguration.JedisClientConfigurationBuilder builder = applyProperties(
                JedisClientConfiguration.builder());
        RedisProperties.Pool pool = this.properties.getJedis().getPool();
        if (pool != null) {
            applyPooling(pool, builder);
        }
        if (StringUtils.hasText(this.properties.getUrl())) {
            customizeConfigurationFromUrl(builder);
        }
        customize(builder);
        return builder.build();
    }

    private JedisClientConfiguration.JedisClientConfigurationBuilder applyProperties(
            JedisClientConfiguration.JedisClientConfigurationBuilder builder) {
        if (this.properties.isSsl()) {
            builder.useSsl();
        }
        if (this.properties.getTimeout() != null) {
            Duration timeout = this.properties.getTimeout();
            builder.readTimeout(timeout).connectTimeout(timeout);
        }
        return builder;
    }

    private void applyPooling(RedisProperties.Pool pool,
                              JedisClientConfiguration.JedisClientConfigurationBuilder builder) {
        builder.usePooling().poolConfig(jedisPoolConfig(pool));
    }

    private JedisPoolConfig jedisPoolConfig(RedisProperties.Pool pool) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(pool.getMaxActive());
        config.setMaxIdle(pool.getMaxIdle());
        config.setMinIdle(pool.getMinIdle());
        if (pool.getMaxWait() != null) {
            config.setMaxWaitMillis(pool.getMaxWait().toMillis());
        }
        return config;
    }

    private void customizeConfigurationFromUrl(
            JedisClientConfiguration.JedisClientConfigurationBuilder builder) {
        RedisSentinelSlaveConnectionConfiguration.ConnectionInfo connectionInfo = parseUrl(this.properties.getUrl());
        if (connectionInfo.isUseSsl()) {
            builder.useSsl();
        }
    }

    private void customize(
            JedisClientConfiguration.JedisClientConfigurationBuilder builder) {
        for (JedisClientConfigurationBuilderCustomizer customizer : this.builderCustomizers) {
            customizer.customize(builder);
        }
    }

}
