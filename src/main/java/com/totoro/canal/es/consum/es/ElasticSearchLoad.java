package com.totoro.canal.es.consum.es;

import com.totoro.canal.es.channel.TotoroChannel;
import com.totoro.canal.es.common.AbstractTotoroLifeCycle;
import com.totoro.canal.es.model.es.ElasticsearchMetadata;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/11/19 下午6:51
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class ElasticSearchLoad extends AbstractTotoroLifeCycle implements Runnable {


    private TotoroChannel channel;

    private int sum = 0;

    public ElasticSearchLoad(TotoroChannel channel) {
        this.channel = channel;
    }

    @Override
    public void start() {
        super.start();
        while (running) {

            try {
                Future<ElasticsearchMetadata> future = channel.takeFuture();
                ElasticsearchMetadata elasticsearchMetadata = future.get();
                System.out.println("future 来了");

                if (elasticsearchMetadata != null) {
                    sum += Integer.valueOf(elasticsearchMetadata.getId());
                    System.out.println("消费数据" + sum);
                } else {
                    System.out.println("数据为空");
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public void run() {
        start();
    }
}
