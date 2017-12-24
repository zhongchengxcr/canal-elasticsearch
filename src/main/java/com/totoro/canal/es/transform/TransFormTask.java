package com.totoro.canal.es.transform;

import com.alibaba.otter.canal.common.utils.BooleanMutex;
import com.alibaba.otter.canal.protocol.Message;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.totoro.canal.es.channel.TotoroChannel;
import com.totoro.canal.es.common.GlobalTask;
import com.totoro.canal.es.common.RollBackMonitorFactory;
import com.totoro.canal.es.common.TotoroObjectPool;
import com.totoro.canal.es.consum.es.ElasticsearchMetadata;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

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

    public TransFormTask(TotoroChannel channel, EsAdapter esAdapter, int threadNum) {

        logger.info("TransFormTask init  start .......");

        this.channel = channel;
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("trans-pool-%d")
                .build();
        executorService = Executors.newFixedThreadPool(threadNum, threadFactory);
        this.esAdapter = esAdapter;

        logger.info("TransFormTask init complete .......");
    }


    @Override
    public void run() {
        logger.info("TransFormTask start .......");
        running = true;
        while (running) {
            try {
                booleanMutex.get();
            } catch (InterruptedException e) {
                logger.error("TransFormTask task has been interrupted ", e);
                running = false;
                break;
            }
            try {
                Message message = channel.takeMessage();
                logger.info("Transform message =====> {}", message.getId());
                TotoroTransForm transForm = getTotoroTransForm(message);
                Future<ElasticsearchMetadata> future = executorService.submit(transForm);
                channel.putFuture(future);
            } catch (InterruptedException e) {
                logger.error("TransFormTask task has been interrupted ", e);
                running = false;
                break;
            }
        }
    }

    private TotoroTransForm getTotoroTransForm(Message message) {
        TotoroTransForm transForm = (TotoroTransForm) TotoroObjectPool.transForm();
        transForm.setEsAdapter(esAdapter);
        transForm.setMessage(message);
        return transForm;
    }

}
