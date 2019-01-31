/**
 * FileName: JedisSentinelSlavePool
 *
 * @author: zhfang
 * Date: 2019/1/30 14:30
 * Description:
 */
package com.minihu.springboot.redis.slave;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.Pool;

import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @CalssName: JedisSentinelSlavePool
 * @Descrption:
 * @Author: zhfang
 * @Date: 2019/1/30 14:30
 **/
public class JedisSentinelSlavePool extends Pool<Jedis> {
    private final String masterName;

    protected GenericObjectPoolConfig poolConfig;

    protected int connectionTimeout = Protocol.DEFAULT_TIMEOUT;
    protected int soTimeout = Protocol.DEFAULT_TIMEOUT;

    protected String password;

    protected int database = Protocol.DEFAULT_DATABASE;

    protected String clientName;

    protected final Set<MasterListener> masterListeners = new HashSet<MasterListener>();

    protected Logger logger = LoggerFactory.getLogger(JedisSentinelSlavePool.class.getName());

    private volatile JedisSentinelSlaveFactory factory;
    private volatile HostAndPort currentSentinel;

    private Set<String> sentinels;

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels,
                                  final GenericObjectPoolConfig poolConfig) {
        this(masterName, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, null,
                Protocol.DEFAULT_DATABASE);
    }

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels) {
        this(masterName, sentinels, new GenericObjectPoolConfig(), Protocol.DEFAULT_TIMEOUT, null,
                Protocol.DEFAULT_DATABASE);
    }

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels, String password) {
        this(masterName, sentinels, new GenericObjectPoolConfig(), Protocol.DEFAULT_TIMEOUT, password);
    }

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels,
                                  final GenericObjectPoolConfig poolConfig, int timeout, final String password) {
        this(masterName, sentinels, poolConfig, timeout, password, Protocol.DEFAULT_DATABASE);
    }

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels,
                                  final GenericObjectPoolConfig poolConfig, final int timeout) {
        this(masterName, sentinels, poolConfig, timeout, null, Protocol.DEFAULT_DATABASE);
    }

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels,
                                  final GenericObjectPoolConfig poolConfig, final String password) {
        this(masterName, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT, password);
    }

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels,
                                  final GenericObjectPoolConfig poolConfig, int timeout, final String password,
                                  final int database) {
        this(masterName, sentinels, poolConfig, timeout, timeout, password, database);
    }

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels,
                                  final GenericObjectPoolConfig poolConfig, int timeout, final String password,
                                  final int database, final String clientName) {
        this(masterName, sentinels, poolConfig, timeout, timeout, password, database, clientName);
    }

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels,
                                  final GenericObjectPoolConfig poolConfig, final int timeout, final int soTimeout,
                                  final String password, final int database) {
        this(masterName, sentinels, poolConfig, timeout, soTimeout, password, database, null);
    }

    public JedisSentinelSlavePool(String masterName, Set<String> sentinels,
                                  final GenericObjectPoolConfig poolConfig, final int connectionTimeout, final int soTimeout,
                                  final String password, final int database, final String clientName) {
        this.poolConfig = poolConfig;
        this.connectionTimeout = connectionTimeout;
        this.soTimeout = soTimeout;
        this.password = password;
        this.database = database;
        this.clientName = clientName;
        this.masterName = masterName;
        this.sentinels = sentinels;

        HostAndPort aSentinel = initsentinels(this.sentinels, masterName);
        initPool(aSentinel);
    }

    @Override
    public void destroy() {
        for (JedisSentinelSlavePool.MasterListener m : masterListeners) {
            m.shutdown();
        }

        super.destroy();
    }

    public HostAndPort getCurrentSentinel() {
        return currentSentinel;
    }

    private void initPool(HostAndPort sentinel) {
        if (!sentinel.equals(currentSentinel)) {
            currentSentinel = sentinel;
            if (factory == null) {
                factory = new JedisSentinelSlaveFactory(sentinel.getHost(), sentinel.getPort(), connectionTimeout,
                        soTimeout, password, database, clientName, false, null, null, null, masterName);
                initPool(poolConfig, factory);
            } else {
                factory.setHostAndPortOfASentinel(currentSentinel);
                internalPool.clear();
            }

            logger.info("Created JedisPool to sentinel at " + sentinel);
        }
    }

    private HostAndPort initsentinels(Set<String> sentinels, final String masterName) {

        HostAndPort aSentinel = null;
        boolean sentinelAvailable = false;

        logger.info("Trying to find a valid sentinel from available Sentinels...");

        for (String sentinelStr : sentinels) {
            final HostAndPort hap = HostAndPort.parseString(sentinelStr);

            logger.info("Connecting to Sentinel " + hap);

            Jedis jedis = null;
            try {
                jedis = new Jedis(hap.getHost(), hap.getPort());
                sentinelAvailable = true;

                List<String> masterAddr = jedis.sentinelGetMasterAddrByName(masterName);
                if (masterAddr == null || masterAddr.size() != 2) {
                    logger.warn("Can not get master addr from sentinel, master name: " + masterName
                            + ". Sentinel: " + hap + ".");
                    continue;
                }

                aSentinel = hap;
                logger.info("Found a Redis Sentinel at " + aSentinel);
                break;
            } catch (JedisException e) {
                logger.warn("Cannot get master address from sentinel running @ " + hap + ". Reason: " + e
                        + ". Trying next one.");
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }

        if (aSentinel == null) {
            if (sentinelAvailable) {
                throw new JedisException("Can connect to sentinel, but " + masterName
                        + " seems to be not monitored...");
            } else {
                throw new JedisConnectionException("All sentinels down, cannot determine where is "
                        + masterName + " master is running...");
            }
        }

        logger.info("Found Redis sentinel running at " + aSentinel + ", starting Sentinel listeners...");

        for (String sentinel : sentinels) {
            final HostAndPort hap = HostAndPort.parseString(sentinel);
            JedisSentinelSlavePool.MasterListener masterListener = new JedisSentinelSlavePool.MasterListener(masterName, hap.getHost(), hap.getPort());
            masterListener.setDaemon(true);
            masterListeners.add(masterListener);
            masterListener.start();
        }

        return aSentinel;
    }


    /**
     * @deprecated starting from Jedis 3.0 this method will not be exposed. Resource cleanup should be
     * done using @see {@link redis.clients.jedis.Jedis#close()}
     */
    @Override
    @Deprecated
    public void returnBrokenResource(final Jedis resource) {
        if (resource != null) {
            returnBrokenResourceObject(resource);
        }
    }

    /**
     * @deprecated starting from Jedis 3.0 this method will not be exposed. Resource cleanup should be
     * done using @see {@link redis.clients.jedis.Jedis#close()}
     */
    @Override
    @Deprecated
    public void returnResource(final Jedis resource) {
        if (resource != null) {
            resource.resetState();
            returnResourceObject(resource);
        }
    }

    private HostAndPort toHostAndPort(List<String> getMasterAddrByNameResult) {
        String host = getMasterAddrByNameResult.get(0);
        int port = Integer.parseInt(getMasterAddrByNameResult.get(1));

        return new HostAndPort(host, port);
    }

    protected class MasterListener extends Thread {

        protected String masterName;
        protected String host;
        protected int port;
        protected long subscribeRetryWaitTimeMillis = 5000;
        protected volatile Jedis j;
        protected AtomicBoolean running = new AtomicBoolean(false);

        protected MasterListener() {
        }

        public MasterListener(String masterName, String host, int port) {
            super(String.format("MasterListener-%s-[%s:%d]", masterName, host, port));
            this.masterName = masterName;
            this.host = host;
            this.port = port;
        }

        public MasterListener(String masterName, String host, int port,
                              long subscribeRetryWaitTimeMillis) {
            this(masterName, host, port);
            this.subscribeRetryWaitTimeMillis = subscribeRetryWaitTimeMillis;
        }

        @Override
        public void run() {

            running.set(true);

            while (running.get()) {

                j = new Jedis(host, port);

                try {
                    // double check that it is not being shutdown
                    if (!running.get()) {
                        break;
                    }

                    j.subscribe(new SentinelSlaveChangePubSub(), "+switch-master", "+slave", "+sdown", "+odown", "+reboot");

                } catch (JedisConnectionException e) {

                    if (running.get()) {
                        logger.error("Lost connection to Sentinel at " + host + ":" + port
                                + ". Sleeping 5000ms and retrying.", e);
                        try {
                            Thread.sleep(subscribeRetryWaitTimeMillis);
                        } catch (InterruptedException e1) {
                            logger.info("Sleep interrupted: ", e1);
                        }
                    } else {
                        logger.info("Unsubscribing from Sentinel at " + host + ":" + port);
                    }
                } finally {
                    j.close();
                }
            }
        }

        public void shutdown() {
            try {
                logger.info("Shutting down listener on " + host + ":" + port);
                running.set(false);
                if (j != null) {
                    j.disconnect();
                }
            } catch (Exception e) {
                logger.error("Caught exception while shutting down: ", e);
            }
        }

        private class SentinelSlaveChangePubSub extends JedisPubSub {
            @Override
            public void onMessage(String channel, String message) {
                if (masterName == null) {
                    logger.error("Master Name is null!");
                    throw new InvalidParameterException("Master Name is null!");
                }
                logger.info("Get message on chanel: " + channel + " published: " + message + "." + " current sentinel " + host + ":" + port);

                String[] msg = message.split(" ");
                List<String> msgList = Arrays.asList(msg);
                if (msgList.isEmpty()) {
                    return;
                }
                boolean needResetPool = false;
                if (masterName.equalsIgnoreCase(msgList.get(0))) {
                    needResetPool = true;
                }
                int tmpIndex = msgList.indexOf("@") + 1;
                if (tmpIndex > 0 && masterName.equalsIgnoreCase(msgList.get(tmpIndex))) {
                    needResetPool = true;
                }
                if (needResetPool) {
                    HostAndPort aSentinel = initsentinels(sentinels, masterName);
                    initPool(aSentinel);
                } else {
                    logger.info("message is not for master " + masterName);
                }

            }
        }
    }
}
