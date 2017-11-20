package com.totoro.canal.es.channel;

import com.alibaba.otter.canal.protocol.Message;
import com.totoro.canal.es.model.es.ElasticsearchMetadata;

import java.util.concurrent.*;

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

    private LinkedBlockingQueue<Message> selectorMessageQueue = new LinkedBlockingQueue<>(50);

    private LinkedBlockingQueue<Future<ElasticsearchMetadata>> transFormFuture = new LinkedBlockingQueue<>(50);


    public void putMessage(Message e) throws InterruptedException {
        selectorMessageQueue.put(e);
    }

    public Message takeMessage() throws InterruptedException {
        return selectorMessageQueue.take();
    }


    public void putFuture(Future<ElasticsearchMetadata> future) throws InterruptedException {
        transFormFuture.put(future);
    }

    public Future<ElasticsearchMetadata> takeFuture() throws InterruptedException {
        return transFormFuture.take();
    }

}
