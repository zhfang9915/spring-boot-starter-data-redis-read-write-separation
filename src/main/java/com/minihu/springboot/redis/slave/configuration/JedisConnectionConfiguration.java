/*
 * Copyright 2012-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.minihu.springboot.redis.slave.configuration;

import com.minihu.springboot.redis.slave.JedisSentinelSlaveConnectionFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.data.redis.JedisClientConfigurationBuilderCustomizer;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration.JedisClientConfigurationBuilder;
import org.springframework.data.redis.connection.jedis.JedisConnection;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.util.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

/**
 * 重写JedisConnectionConfiguration，改变注入JedisConnectionFactory的条件
 *
 * @author zhfang
 */
@Configuration
@EnableConfigurationProperties(RedisProperties.class)
@ConditionalOnClass({ GenericObjectPool.class, JedisConnection.class, Jedis.class })
class JedisConnectionConfiguration extends RedisConnectionConfiguration {

	private final RedisProperties properties;

	private final List<JedisClientConfigurationBuilderCustomizer> builderCustomizers;

	JedisConnectionConfiguration(RedisProperties properties,
                                 ObjectProvider<RedisSentinelConfiguration> sentinelConfiguration,
                                 ObjectProvider<RedisClusterConfiguration> clusterConfiguration,
                                 ObjectProvider<List<JedisClientConfigurationBuilderCustomizer>> builderCustomizers) {
		super(properties, sentinelConfiguration, clusterConfiguration);
		this.properties = properties;
		this.builderCustomizers = builderCustomizers
				.getIfAvailable(Collections::emptyList);
	}

	/**
	 * 将注入条件更改为@ConditionalOnBean(JedisSentinelSlaveConnectionFactory.class)
	 * 当有读连接配置的时候加载写连接配置
	 *
	 * 原条件为@ConditionalOnMissingBean(RedisConnectionFactory.class)
	 * 若配置了读连接JedisSentinelSlaveConnectionFactory，因为都实现了RedisConnectionFactory，
	 * 所以会认为容器中已经有连接池的配置，将不加载写连接的配置
	 * @return
	 */
	@Bean
	@ConditionalOnBean(JedisSentinelSlaveConnectionFactory.class)
	public JedisConnectionFactory redisConnectionFactory(){
		return createJedisConnectionFactory();
	}

	private JedisConnectionFactory createJedisConnectionFactory() {
		JedisClientConfiguration clientConfiguration = getJedisClientConfiguration();
		if (getSentinelConfig() != null) {
			return new JedisConnectionFactory(getSentinelConfig(), clientConfiguration);
		}
		if (getClusterConfiguration() != null) {
			return new JedisConnectionFactory(getClusterConfiguration(),
					clientConfiguration);
		}
		return new JedisConnectionFactory(getStandaloneConfig(), clientConfiguration);
	}

	private JedisClientConfiguration getJedisClientConfiguration() {
		JedisClientConfigurationBuilder builder = applyProperties(
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

	private JedisClientConfigurationBuilder applyProperties(
			JedisClientConfigurationBuilder builder) {
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
			JedisClientConfigurationBuilder builder) {
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
			JedisClientConfigurationBuilder builder) {
		ConnectionInfo connectionInfo = parseUrl(this.properties.getUrl());
		if (connectionInfo.isUseSsl()) {
			builder.useSsl();
		}
	}

	private void customize(
			JedisClientConfigurationBuilder builder) {
		for (JedisClientConfigurationBuilderCustomizer customizer : this.builderCustomizers) {
			customizer.customize(builder);
		}
	}

}
