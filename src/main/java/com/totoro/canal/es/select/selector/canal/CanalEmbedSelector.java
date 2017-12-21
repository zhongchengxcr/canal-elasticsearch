package com.totoro.canal.es.select.selector.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.common.utils.BooleanMutex;
import com.alibaba.otter.canal.protocol.Message;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.totoro.canal.es.common.RollBackMonitorFactory;
import com.totoro.canal.es.common.TotoroException;
import com.totoro.canal.es.select.selector.CanalConf;
import com.totoro.canal.es.select.selector.TotoroSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.net.util.IPAddressUtil;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * <p>
 * Copyright: Copyright (c)
 * <p>
 * Company: xx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class CanalEmbedSelector implements TotoroSelector {

    private static final Logger logger = LoggerFactory.getLogger(CanalEmbedSelector.class);

    private volatile boolean running = false;

    private CanalConnector connector; // instance client

    private String destination;

    private static final int maxEmptyTimes = 10;

    private int batchSize = 5 * 1024;

    private String filterPatten;

    private long batchTimeout = -1L;

    private Mode mode;

    private BooleanMutex rollBack = RollBackMonitorFactory.getBooleanMutex();

    public enum Mode {
        SIGN, CLUSTER
    }

    public CanalEmbedSelector(CanalConf conf) {

        logger.info("TotoroSelector init start  , conf :{}", conf.toString());

        this.mode = conf.getMode();
        this.destination = conf.getDestination();
        this.filterPatten = conf.getFilterPatten();

        String userName = conf.getUserName();
        String passWord = conf.getPassWord();

        if (Mode.SIGN.equals(mode)) {
            String address = conf.getAddress();

            String[] hostPort = address.split(":");

            String ip = hostPort[0];
            Integer port = Integer.valueOf(hostPort[1]);

            SocketAddress socketAddress = new InetSocketAddress(ip, port);

            connector = CanalConnectors.newSingleConnector(socketAddress,
                    destination,
                    userName,
                    passWord);

        } else if (Mode.CLUSTER.equals(mode)) {
            String zkAddress = conf.getZkAddress();
            connector = CanalConnectors.newClusterConnector(zkAddress, destination, userName, passWord);
        } else {
            throw new TotoroException("Invalid mode");
        }
        logger.info("TotoroSelector init complete .......");
    }


    @Override
    public void start() {
        if (running) {
            return;
        }
        connector.connect();
        connector.subscribe(filterPatten);
        running = true;
    }

    @Override
    public boolean isStart() {
        return running;
    }

    @Override
    public void stop() {
        connector.disconnect();
    }

    @Override
    public Message selector() throws InterruptedException {
        if (!running) {
            throw new RuntimeException("CanalEmbedSelector has benn not start");
        }

        Message message = null;
        int emptyTimes = 0;

        if (batchTimeout < 0) {
            while (running) {
                message = connector.getWithoutAck(batchSize);

                if (message == null || message.getId() == -1L) {
                    if (rollBack.state() == false) {
                        break;
                    } else {
                        applyWait(emptyTimes++);
                    }

                } else {
                    break;
                }
            }
            if (!running) {
                throw new InterruptedException();
            }
        } else {

            while (running) {
                message = connector.getWithoutAck(batchSize, batchTimeout, TimeUnit.SECONDS);
                if (message == null || message.getId() == -1L) {
                    continue;
                } else {
                    break;
                }
            }
            if (!running) {
                throw new InterruptedException();
            }
        }

        return message;
    }

    @Override
    public Long lastEntryTime() {
        return null;
    }

    @Override
    public List<Long> unAckBatchs() {
        return null;
    }

    @Override
    public void rollback(Long batchId) {
        connector.rollback(batchId);
    }

    @Override
    public void rollback() {
        connector.rollback();
    }

    @Override
    public void ack(Long batchId) {
        connector.ack(batchId);
    }


    private void applyWait(int emptyTimes) throws InterruptedException {
        int newEmptyTimes = emptyTimes > maxEmptyTimes ? maxEmptyTimes : emptyTimes;
        if (emptyTimes >= 3) {
            Thread.yield();
            LockSupport.parkNanos(10000 * 1000L * newEmptyTimes);
        }
    }


}
