package com.totoro.canal.es.select.selector;

import com.alibaba.otter.canal.protocol.Message;

import java.util.List;

/**
 * 标题、简要说明. <br>
 * 类详细说明.
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
     * 启动
     */
    void start();

    /**
     * 是否启动
     */
    boolean isStart();

    /**
     * 关闭
     */
    void stop();

    /**
     * 获取一批待处理的数据
     */
    Message selector() throws InterruptedException;


    /**
     * 返回最后一次entry数据的时间戳
     */
    Long lastEntryTime();

    /**
     * 返回未被ack的数据
     */
    List<Long> unAckBatchs();

    /**
     * 反馈一批数据处理失败，需要下次重新被处理
     */
    void rollback(Long batchId);

    /**
     * 反馈所有的batch数据需要被重新处理
     */
    void rollback();

    /**
     * 反馈一批数据处理完成
     */
    void ack(Long batchId);
}
