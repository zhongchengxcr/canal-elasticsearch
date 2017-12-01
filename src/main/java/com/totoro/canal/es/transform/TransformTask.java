package com.totoro.canal.es.transform;

import com.totoro.canal.es.channel.TotoroChannel;
import com.totoro.canal.es.common.AbstractTotoroLifeCycle;
import com.totoro.canal.es.model.es.ElasticsearchMetadata;

import java.util.concurrent.Future;

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

    private TransFormTask(TotoroChannel channel) {
        this.channel = channel;
        transFormExecutor = new TransFormExecutor();
    }

    @Override
    public void start() {
        super.start();

        TotoroTransForm transForm = new TotoroTransForm(channel);

        Future<ElasticsearchMetadata> future = transFormExecutor.submit(transForm);

        try {
            channel.putFuture(future);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
