package com.totoro.canal.es.select.selector;

import com.alibaba.otter.canal.protocol.Message;

import java.util.List;

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
public interface TotoroSelector {

    /**
     */
    void start();

    /**
     */
    boolean isStart();

    /**
     */
    void stop();

    /**
     */
    Message selector() throws InterruptedException;


    /**
     */
    Long lastEntryTime();

    /**
     */
    List<Long> unAckBatchs();

    /**
     */
    void rollback(Long batchId);

    /**
     */
    void rollback();

    /**
     */
    void ack(Long batchId);
}
