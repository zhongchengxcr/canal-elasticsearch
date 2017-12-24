package com.totoro.canal.es.channel;

import com.alibaba.otter.canal.common.utils.BooleanMutex;
import com.alibaba.otter.canal.protocol.Message;
import com.totoro.canal.es.common.RollBackMonitorFactory;
import com.totoro.canal.es.consum.es.ElasticsearchMetadata;
import com.totoro.canal.es.select.selector.TotoroSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

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
public class TotoroChannel {

    private BooleanMutex rollBack = RollBackMonitorFactory.getBooleanMutex();

    private Logger logger = LoggerFactory.getLogger(TotoroChannel.class);

    private LinkedBlockingQueue<Message> selectorMessageQueue = new LinkedBlockingQueue<>(5);

    private LinkedBlockingQueue<Future<ElasticsearchMetadata>> transFormFuture = new LinkedBlockingQueue<>(5);


    private TotoroSelector totoroSelector;

    public TotoroChannel(TotoroSelector totoroSelector) {
        this.totoroSelector = totoroSelector;
    }

    public void ack(Long batchId) {
        totoroSelector.ack(batchId);
    }

    /**
     * 处于回滚状态下，拒绝所有put的消息
     */

    public void clearMessage() {
        selectorMessageQueue.clear();
        transFormFuture.clear();
    }


    public void putMessage(Message e) throws InterruptedException {
        if (rollBack.state() == true) {
            selectorMessageQueue.put(e);
        } else {
            logger.info("The rollback happened =============>  discard message , batchId :{}", e.getId());
        }
    }

    public Message takeMessage() throws InterruptedException {
        return selectorMessageQueue.take();
    }

    public void putFuture(Future<ElasticsearchMetadata> future) throws InterruptedException {
        if (rollBack.state() == true) {
            transFormFuture.put(future);
        } else {
            future.cancel(true);
            logger.info("The rollback happened =============>  try cancel future ");
        }
    }

    public Future<ElasticsearchMetadata> take() throws InterruptedException {
        return transFormFuture.take();
    }


    public void close() {
        Object[] tuple2Arr = transFormFuture.toArray();
        for (Object obj : tuple2Arr) {
            Future<ElasticsearchMetadata> future = (Future<ElasticsearchMetadata>) obj;
            future.cancel(true);
        }
    }

}
