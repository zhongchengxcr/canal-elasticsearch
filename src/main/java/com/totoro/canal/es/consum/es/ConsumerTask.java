package com.totoro.canal.es.consum.es;

import com.alibaba.otter.canal.common.utils.BooleanMutex;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.totoro.canal.es.channel.TotoroChannel;
import com.totoro.canal.es.common.GlobalTask;
import com.totoro.canal.es.common.RollBackMonitorFactory;

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
        logger.info("Consumer task init start .......");
        this.channel = totoroChannel;
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("esload-pool-%d").build();
        executorService = Executors.newSingleThreadExecutor(threadFactory);

        logger.info("Consumer task init complete.......");
    }

    @Override
    public void run() {
        logger.info("ConsumerTask start .......");
        running = true;
        while (running) {
            try {
                rollBack.get();
            } catch (InterruptedException e) {
                logger.error("Consumer task has benn interrupted ");
                running = false;
                break;
            }
            ElasticsearchMetadata elasticsearchMetadata = null;
            try {
                Future<ElasticsearchMetadata> future = channel.take();


                elasticsearchMetadata = future.get();
                logger.info("Consumer message start =====> {}", elasticsearchMetadata.getBatchId());

                ElasticsearchMetadata finalElasticsearchMetadata = elasticsearchMetadata;

                consumers.forEach(consumer -> consumer.consume(finalElasticsearchMetadata));

                channel.ack(elasticsearchMetadata.getBatchId());

                logger.info("Consumer message ack =====> {}", elasticsearchMetadata.getBatchId());

            } catch (Exception e) {

                if (e instanceof InterruptedException) {
                    logger.error("Trans form thread has been interrupted ", e);
                } else if (e instanceof ExecutionException) {
                    logger.error("Trans form callable exception ", e);
                } else {
                    logger.error("Consumer catch unknow exception ", e);
                }

                logger.error("Exception occurred , Call roll back!");
                rollBack.set(false); //回滚
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e1) {
                    logger.error("Consumer task has benn interrupted ");
                    running = false;
                    break;
                }
            } finally {
                if (elasticsearchMetadata != null) {
                    elasticsearchMetadata.recycle();
                }

            }
        }
    }

    public void register(Consumer consumer) {
        consumers.add(consumer);
    }


}
