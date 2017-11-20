package com.totoro.canal.es;

import com.alibaba.otter.canal.protocol.Message;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.totoro.canal.es.channel.TotoroChannel;
import com.totoro.canal.es.consum.es.ElasticSearchLoad;
import com.totoro.canal.es.model.es.ElasticsearchMetadata;
import com.totoro.canal.es.select.selector.canal.CanalEmbedSelector;
import com.totoro.canal.es.transform.TotoroTransForm;

import java.util.Properties;
import java.util.concurrent.*;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/11/19 下午6:00
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class CanalScheduler {

    private Properties conf;

    private volatile boolean running = false;

    public CanalScheduler(final Properties conf) {
        this.conf = conf;
    }


    public void start() throws InterruptedException, ExecutionException {
        running = true;
        TotoroChannel channel = new TotoroChannel();

        CanalEmbedSelector canalEmbedSelector = new CanalEmbedSelector();
        canalEmbedSelector.start();
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("trans-pool-%d").build();


        ExecutorService loadExcuter = Executors.newSingleThreadExecutor();

        ElasticSearchLoad loader = new ElasticSearchLoad(channel);

        loadExcuter.submit(loader);

        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(10
                , 10
                , 0L
                , TimeUnit.SECONDS
                , new LinkedBlockingQueue<>(50)
                , threadFactory);

        while (running) {
            Message message = canalEmbedSelector.selector();

            long batchId = message.getId();
            int size = message.getEntries().size();
            if (batchId == -1 || size == 0) {
                // try {
                // Thread.sleep(1000);
                // } catch (InterruptedException e) {
                // }

                System.out.println("空数据");
            } else {
                channel.putMessage(message);

                System.out.println("放入数据 " + message.getId());
                TotoroTransForm transForm = new TotoroTransForm(channel);
                Future<ElasticsearchMetadata> future = threadPoolExecutor.submit(transForm);
                channel.putFuture(future);
                canalEmbedSelector.ack(batchId); // 提交确认
            }


        }


    }

    public void stop() {

    }


}
