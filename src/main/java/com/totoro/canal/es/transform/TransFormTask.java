package com.totoro.canal.es.transform;

import com.alibaba.otter.canal.protocol.Message;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.totoro.canal.es.channel.TotoroChannel;
import com.totoro.canal.es.common.AbstractTotoroLifeCycle;
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
public class TransFormTask extends AbstractTotoroLifeCycle {

    private TotoroChannel channel;

    private TransFormExecutor transFormExecutor;

    private ExecutorService executorService;

    public TransFormTask(TotoroChannel channel) {
        this.channel = channel;
        transFormExecutor = new TransFormExecutor();

        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("trans-pool-%d").build();

        executorService = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), threadFactory);

    }

    @Override
    public void start() {
        super.start();
        executorService.submit(() -> {
            while (running) {
                try {
                    Message message = channel.takeMessage();
                    System.out.println("数据来了 ：" + message);
                    TotoroTransForm transForm = new TotoroTransForm(message);
                    Future<ElasticsearchMetadata> future = transFormExecutor.submit(transForm);
                    channel.putFuture(future);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    @Override
    public void stop() {
        super.stop();
        transFormExecutor.shutDown();
    }
}
