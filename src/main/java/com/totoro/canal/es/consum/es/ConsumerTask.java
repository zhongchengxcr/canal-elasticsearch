package com.totoro.canal.es.consum.es;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.totoro.canal.es.channel.TotoroChannel;
import com.totoro.canal.es.common.AbstractTotoroLifeCycle;
import com.totoro.canal.es.common.task.GlobalTask;
import com.totoro.canal.es.model.es.ElasticsearchMetadata;

import java.util.concurrent.*;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/12/01 下午12:49
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class ConsumerTask extends GlobalTask {

    private Consumer consumer;

    private TotoroChannel channel;

    public ConsumerTask(TotoroChannel totoroChannel, Consumer consumer) {
        this.channel = totoroChannel;
        this.consumer = consumer;
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("esload-pool-%d").build();
        executorService = Executors.newSingleThreadExecutor(threadFactory);
    }


    @Override
    public void run() {
        running = true;
        while (running) {
            try {
                Future<ElasticsearchMetadata> future = channel.takeFuture();
                ElasticsearchMetadata elasticsearchMetadata = future.get();
                consumer.consume(elasticsearchMetadata);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

}
