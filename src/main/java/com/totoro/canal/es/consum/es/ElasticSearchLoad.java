package com.totoro.canal.es.consum.es;

import com.totoro.canal.es.channel.TotoroChannel;
import com.totoro.canal.es.model.es.ElasticsearchMetadata;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

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
public class ElasticSearchLoad implements Runnable {

    private volatile boolean running = false;

    private TotoroChannel channel;

    private AtomicInteger atomicInteger = new AtomicInteger();

    private int sum = 0;

    public ElasticSearchLoad(TotoroChannel channel) {
        this.channel = channel;
    }


    public void start() throws InterruptedException, ExecutionException {
        running = true;

        while (running) {
            Future<ElasticsearchMetadata> future = null;
            future = channel.takeFuture();


            ElasticsearchMetadata elasticsearchMetadata = future.get();

            if (elasticsearchMetadata != null) {
                sum += Integer.valueOf(elasticsearchMetadata.getId());
                System.out.println("消费数据" + sum);
            } else {
                System.out.println("数据为空");
            }
        }
    }


    public void stop() {
        running = false;
    }

    @Override
    public void run() {
        try {
            start();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
