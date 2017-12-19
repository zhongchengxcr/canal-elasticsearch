package com.totoro.canal.es.transform;

import com.alibaba.otter.canal.common.utils.BooleanMutex;
import com.alibaba.otter.canal.protocol.Message;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.totoro.canal.es.channel.TotoroChannel;
import com.totoro.canal.es.common.RollBackMonitorFactory;
import com.totoro.canal.es.common.task.GlobalTask;
import com.totoro.canal.es.model.es.ElasticsearchMetadata;

import java.util.concurrent.*;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/12/01 下午1:27
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class TransFormTask extends GlobalTask {

    private TotoroChannel channel;

    private BooleanMutex booleanMutex = RollBackMonitorFactory.getBooleanMutex();

    private EsAdapter esAdapter;


    public TransFormTask(TotoroChannel channel, EsAdapter esAdapter) {

        logger.info("TransFormTask init .......");

        this.channel = channel;
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("trans-pool-%d")
                .build();
        executorService = Executors.newFixedThreadPool(10, threadFactory);
        this.esAdapter = esAdapter;

        logger.info("TransFormTask init complete.......");
    }


    @Override
    public void run() {
        running = true;
        while (running) {
            try {
                booleanMutex.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }
            try {
                Message message = channel.takeMessage();
                logger.info("数据来了 ：" + message.getId());
                TotoroTransForm transForm = new TotoroTransForm(message, esAdapter);
                Future<ElasticsearchMetadata> future = executorService.submit(transForm);
                channel.putFuture(future);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
