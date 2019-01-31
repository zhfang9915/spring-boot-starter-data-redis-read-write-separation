/**
 * FileName: JedisSentinelSlaveFactory
 *
 * @author: zhfang
 * Date: 2019/1/30 14:26
 * Description:
 */
package com.minihu.springboot.redis.slave;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.InvalidURIException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.JedisURIHelper;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import java.net.URI;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;


/**
 *@CalssName: JedisSentinelSlaveFactory
 *@Descrption:
 *@Author: zhfang
 *@Date: 2019/1/30 14:26
 **/
public class JedisSentinelSlaveFactory implements PooledObjectFactory<Jedis> {
    private final  String masterName;
    private final int retryTimeWhenRetrieveSlave = 5;

    private final AtomicReference<HostAndPort> hostAndPortOfASentinel = new AtomicReference<HostAndPort>();
    private final int connectionTimeout;
    private final int soTimeout;
    private final String password;
    private final int database;
    private final String clientName;
    private final boolean ssl;
    private final SSLSocketFactory sslSocketFactory;
    private SSLParameters sslParameters;
    private HostnameVerifier hostnameVerifier;

    public JedisSentinelSlaveFactory(final String host, final int port, final int connectionTimeout,
                                     final int soTimeout, final String password, final int database, final String clientName,
                                     final boolean ssl, final SSLSocketFactory sslSocketFactory, final SSLParameters sslParameters,
                                     final HostnameVerifier hostnameVerifier,String masterName) {
        this.hostAndPortOfASentinel.set(new HostAndPort(host, port));
        this.connectionTimeout = connectionTimeout;
        this.soTimeout = soTimeout;
        this.password = password;
        this.database = database;
        this.clientName = clientName;
        this.ssl = ssl;
        this.sslSocketFactory = sslSocketFactory;
        this.sslParameters = sslParameters;
        this.hostnameVerifier = hostnameVerifier;
        this.masterName = masterName;
    }

    public JedisSentinelSlaveFactory(final URI uri, final int connectionTimeout, final int soTimeout,
                                     final String clientName, final boolean ssl, final SSLSocketFactory sslSocketFactory,
                                     final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier,String masterName) {
        if (!JedisURIHelper.isValid(uri)) {
            throw new InvalidURIException(String.format(
                    "Cannot open Redis connection due invalid URI. %s", uri.toString()));
        }

        this.hostAndPortOfASentinel.set(new HostAndPort(uri.getHost(), uri.getPort()));
        this.connectionTimeout = connectionTimeout;
        this.soTimeout = soTimeout;
        this.password = JedisURIHelper.getPassword(uri);
        this.database = JedisURIHelper.getDBIndex(uri);
        this.clientName = clientName;
        this.ssl = ssl;
        this.sslSocketFactory = sslSocketFactory;
        this.sslParameters = sslParameters;
        this.hostnameVerifier = hostnameVerifier;
        this.masterName = masterName;
    }

    public void setHostAndPortOfASentinel(final HostAndPort hostAndPortOfASentinel) {
        this.hostAndPortOfASentinel.set(hostAndPortOfASentinel);
    }

    @Override
    public void activateObject(PooledObject<Jedis> pooledJedis) throws Exception {
        final BinaryJedis jedis = pooledJedis.getObject();
        if (jedis.getDB() != database) {
            jedis.select(database);
        }

    }

    @Override
    public void destroyObject(PooledObject<Jedis> pooledJedis) throws Exception {
        final BinaryJedis jedis = pooledJedis.getObject();
        if (jedis.isConnected()) {
            try {
                try {
                    jedis.quit();
                } catch (Exception e) {
                }
                jedis.disconnect();
            } catch (Exception e) {

            }
        }

    }

    @Override
    public PooledObject<Jedis> makeObject() throws Exception {
        final Jedis jedisSentinel = getASentinel();

        List<Map<String,String>> slaves = jedisSentinel.sentinelSlaves(this.masterName);
        if(slaves == null || slaves.isEmpty()) {
            throw new JedisException(String.format("No valid slave for master: %s",this.masterName));
        }

        DefaultPooledObject<Jedis> result = tryToGetSlave(slaves);

        if(null != result) {
            return result;
        } else {
            throw new JedisException(String.format("No valid slave for master: %s, after try %d times.",
                    this.masterName,retryTimeWhenRetrieveSlave));
        }

    }

    private DefaultPooledObject<Jedis> tryToGetSlave(List<Map<String,String>> slaves) {
        SecureRandom sr = new SecureRandom();
        int retry = retryTimeWhenRetrieveSlave;
        while(retry >= 0) {
            retry--;
            int randomIndex = sr.nextInt(slaves.size());
            String host = slaves.get(randomIndex).get("ip");
            String port = slaves.get(randomIndex).get("port");
            final Jedis jedisSlave = new Jedis(host,Integer.valueOf(port), connectionTimeout,soTimeout,
                    ssl, sslSocketFactory,sslParameters, hostnameVerifier);
            try {
                jedisSlave.connect();
                if (null != this.password) {
                    jedisSlave.auth(this.password);
                }
                if (database != 0) {
                    jedisSlave.select(database);
                }
                if (clientName != null) {
                    jedisSlave.clientSetname(clientName);
                }
                return  new DefaultPooledObject<Jedis>(jedisSlave);

            } catch (Exception e) {
                jedisSlave.close();
                slaves.remove(randomIndex);
                continue;
            }
        }

        return null;
    }

    private Jedis getASentinel() {
        final HostAndPort hostAndPort = this.hostAndPortOfASentinel.get();
        final Jedis jedis = new Jedis(hostAndPort.getHost(), hostAndPort.getPort(), connectionTimeout,
                soTimeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);

        try {
            jedis.connect();
        } catch (JedisException je) {
            jedis.close();
            throw je;
        }
        return jedis;
    }

    @Override
    public void passivateObject(PooledObject<Jedis> pooledJedis) throws Exception {

    }

    @Override
    public boolean validateObject(PooledObject<Jedis> pooledJedis) {
        final BinaryJedis jedis = pooledJedis.getObject();
        try {
            HostAndPort hostAndPort = this.hostAndPortOfASentinel.get();

            String connectionHost = jedis.getClient().getHost();
            int connectionPort = jedis.getClient().getPort();

            return hostAndPort.getHost().equals(connectionHost)
                    && hostAndPort.getPort() == connectionPort && jedis.isConnected()
                    && jedis.ping().equals("PONG");
        } catch (final Exception e) {
            return false;
        }
    }
}
