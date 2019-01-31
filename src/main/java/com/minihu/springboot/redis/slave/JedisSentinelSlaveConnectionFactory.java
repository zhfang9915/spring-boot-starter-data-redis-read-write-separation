/**
 * FileName: JedisSentinelSlaveConnectionFactory
 *
 * @author: zhfang
 * Date: 2019/1/30 14:32
 * Description:
 */
package com.minihu.springboot.redis.slave;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.Protocol;
import redis.clients.util.Pool;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import java.time.Duration;
import java.util.*;

/**
 * @CalssName: JedisSentinelSlaveConnectionFactory
 * @Descrption:
 * @Author: zhfang
 * @Date: 2019/1/30 14:32
 **/
public class JedisSentinelSlaveConnectionFactory extends JedisConnectionFactory {

    public JedisSentinelSlaveConnectionFactory(RedisSentinelConfiguration sentinelConfig) {
        super(sentinelConfig);
    }

    public JedisSentinelSlaveConnectionFactory(RedisSentinelConfiguration sentinelConfig, JedisClientConfiguration clientConfig) {
        super(sentinelConfig, clientConfig);
    }

    public JedisSentinelSlaveConnectionFactory(RedisSentinelConfiguration sentinelConfig, JedisPoolConfig poolConfig) {
        super(sentinelConfig, poolConfig);
    }


    @Override
    protected Pool<Jedis> createRedisSentinelPool(RedisSentinelConfiguration config) {
        GenericObjectPoolConfig poolConfig = getPoolConfig() != null ? getPoolConfig() : new JedisPoolConfig();
        return new JedisSentinelSlavePool(config.getMaster().getName(), convertToJedisSentinelSet(config.getSentinels()),
                poolConfig, getConnectTimeout(), getReadTimeout(), getPassword(), getDatabase(), getClientName());
    }

    private int getConnectTimeout() {
        return Math.toIntExact(getClientConfiguration().getConnectTimeout().toMillis());
    }

    private Set<String> convertToJedisSentinelSet(Collection<RedisNode> nodes) {

        if (CollectionUtils.isEmpty(nodes)) {
            return Collections.emptySet();
        }

        Set<String> convertedNodes = new LinkedHashSet<>(nodes.size());
        for (RedisNode node : nodes) {
            if (node != null) {
                convertedNodes.add(node.asString());
            }
        }
        return convertedNodes;
    }

    private int getReadTimeout() {
        return Math.toIntExact(getClientConfiguration().getReadTimeout().toMillis());
    }

    static class MutableJedisClientConfiguration implements JedisClientConfiguration {

        private boolean useSsl;
        private @Nullable
        SSLSocketFactory sslSocketFactory;
        private @Nullable
        SSLParameters sslParameters;
        private @Nullable
        HostnameVerifier hostnameVerifier;
        private boolean usePooling = true;
        private GenericObjectPoolConfig poolConfig = new JedisPoolConfig();
        private @Nullable
        String clientName;
        private Duration readTimeout = Duration.ofMillis(Protocol.DEFAULT_TIMEOUT);
        private Duration connectTimeout = Duration.ofMillis(Protocol.DEFAULT_TIMEOUT);

        public static JedisClientConfiguration create(JedisShardInfo shardInfo) {

            JedisSentinelSlaveConnectionFactory.MutableJedisClientConfiguration configuration = new JedisSentinelSlaveConnectionFactory.MutableJedisClientConfiguration();
            configuration.setShardInfo(shardInfo);
            return configuration;
        }

        public static JedisClientConfiguration create(GenericObjectPoolConfig jedisPoolConfig) {

            JedisSentinelSlaveConnectionFactory.MutableJedisClientConfiguration configuration = new JedisSentinelSlaveConnectionFactory.MutableJedisClientConfiguration();
            configuration.setPoolConfig(jedisPoolConfig);
            return configuration;
        }

        @Override
        public boolean isUseSsl() {
            return useSsl;
        }

        public void setUseSsl(boolean useSsl) {
            this.useSsl = useSsl;
        }

        @Override
        public Optional<SSLSocketFactory> getSslSocketFactory() {
            return Optional.ofNullable(sslSocketFactory);
        }

        public void setSslSocketFactory(SSLSocketFactory sslSocketFactory) {
            this.sslSocketFactory = sslSocketFactory;
        }

        @Override
        public Optional<SSLParameters> getSslParameters() {
            return Optional.ofNullable(sslParameters);
        }

        public void setSslParameters(SSLParameters sslParameters) {
            this.sslParameters = sslParameters;
        }

        @Override
        public Optional<HostnameVerifier> getHostnameVerifier() {
            return Optional.ofNullable(hostnameVerifier);
        }

        public void setHostnameVerifier(HostnameVerifier hostnameVerifier) {
            this.hostnameVerifier = hostnameVerifier;
        }

        @Override
        public boolean isUsePooling() {
            return usePooling;
        }

        public void setUsePooling(boolean usePooling) {
            this.usePooling = usePooling;
        }

        @Override
        public Optional<GenericObjectPoolConfig> getPoolConfig() {
            return Optional.ofNullable(poolConfig);
        }

        public void setPoolConfig(GenericObjectPoolConfig poolConfig) {
            this.poolConfig = poolConfig;
        }

        @Override
        public Optional<String> getClientName() {
            return Optional.ofNullable(clientName);
        }

        public void setClientName(String clientName) {
            this.clientName = clientName;
        }

        @Override
        public Duration getReadTimeout() {
            return readTimeout;
        }

        public void setReadTimeout(Duration readTimeout) {
            this.readTimeout = readTimeout;
        }

        @Override
        public Duration getConnectTimeout() {
            return connectTimeout;
        }

        public void setConnectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
        }

        public void setShardInfo(JedisShardInfo shardInfo) {

            setSslSocketFactory(shardInfo.getSslSocketFactory());
            setSslParameters(shardInfo.getSslParameters());
            setHostnameVerifier(shardInfo.getHostnameVerifier());
            setUseSsl(shardInfo.getSsl());
            setConnectTimeout(Duration.ofMillis(shardInfo.getConnectionTimeout()));
            setReadTimeout(Duration.ofMillis(shardInfo.getSoTimeout()));
        }
    }
}
