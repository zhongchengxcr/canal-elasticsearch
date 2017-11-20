package com.totoro.canal.es.select.selector.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.alibaba.otter.canal.protocol.Message;
import com.totoro.canal.es.select.selector.TotoroSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
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

    private volatile long lastEntryTime = 0;

    private static final int maxEmptyTimes = 10;

    private int batchSize = 5 * 1024;

    private String FILTER_PATTEN = ".*\\..*";

    private long batchTimeout = -1L;

    public CanalEmbedSelector() {
        String ip = AddressUtils.getHostIp();
        connector = CanalConnectors.newSingleConnector(new InetSocketAddress(ip, 11111),
                "totoro",
                "",
                "");
    }

    /**
     */
    protected Thread.UncaughtExceptionHandler handler = (t, e) -> logger.error("parse events has an error", e);

    @Override
    public void start() {
        if (running) {
            return;
        }
        connector.connect();
        connector.subscribe();
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

        if (batchTimeout < 0) {//
            while (running) {
                message = connector.getWithoutAck(batchSize);
                //System.out.println("=========" + message);
                if (message == null || message.getId() == -1L) {
                    applyWait(emptyTimes++);
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
        //Thread.sleep(1000);
        if (emptyTimes <= 3) {
            Thread.yield();
            LockSupport.parkNanos(1000 * 1000L  * newEmptyTimes);
        }
    }


}
