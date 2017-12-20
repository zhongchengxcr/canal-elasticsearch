package com.totoro.canal.es.consum.es;

import com.alibaba.otter.canal.common.utils.BooleanMutex;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.totoro.canal.es.channel.TotoroChannel;
import com.totoro.canal.es.common.RollBackMonitorFactory;
import com.totoro.canal.es.common.GlobalTask;

import java.util.List;
import java.util.Set;
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

    private final Set<Consumer> consumers = new ConcurrentHashMap<Consumer, Boolean>().keySet(true);

    private TotoroChannel channel;

    private BooleanMutex rollBack = RollBackMonitorFactory.getBooleanMutex();

    public ConsumerTask(TotoroChannel totoroChannel) {
        logger.info("ConsumerTask init .......");
        this.channel = totoroChannel;
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("esload-pool-%d").build();
        executorService = Executors.newSingleThreadExecutor(threadFactory);

        logger.info("ConsumerTask init complete.......");
    }

    @Override
    public void run() {
        running = true;
        while (running) {
            try {
                rollBack.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
            try {
                Future<ElasticsearchMetadata> future = channel.takeFuture();
                //如果回滚则无限的丢弃任务
                ElasticsearchMetadata elasticsearchMetadata = future.get();

                logger.info("消费数据 =====" + elasticsearchMetadata.getBatchId());

                consumers.forEach(consumer -> consumer.consume(elasticsearchMetadata));

                channel.ack(elasticsearchMetadata.getBatchId());

            } catch (InterruptedException | ExecutionException e) {
                rollBack.set(false); //回滚
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    public void register(Consumer consumer) {
        consumers.add(consumer);
    }


}
