package com.totoro.canal.es.consum.es;

import com.alibaba.otter.canal.common.utils.BooleanMutex;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.totoro.canal.es.channel.TotoroChannel;
import com.totoro.canal.es.common.RollBackMonitorFactory;
import com.totoro.canal.es.common.task.GlobalTask;
import com.totoro.canal.es.model.es.ElasticsearchMetadata;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

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

    private BooleanMutex rollBack = RollBackMonitorFactory.getBooleanMutex();

    public ConsumerTask(TotoroChannel totoroChannel, Consumer consumer) {

        logger.info("ConsumerTask init .......");

        this.channel = totoroChannel;
        this.consumer = consumer;
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
                consumer.consume(elasticsearchMetadata);


                channel.ack(elasticsearchMetadata.getBatchId());

                //测试
//                if (elasticsearchMetadata.getBatchId() % 2 == 0) {
//                    logger.info("要回滚了");
//                    rollBack.set(false);
//                    //channel.rollback(new RollBackEvent(elasticsearchMetadata.getBatchId()));
//                    //Thread.sleep(500);
//                } else {
//
//                    logger.info("应答 ：" + elasticsearchMetadata.getBatchId());
//                }


            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                //roll back
            }
        }
    }

}
