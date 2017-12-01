package com.totoro.canal.es;

import com.alibaba.otter.canal.protocol.Message;
import com.totoro.canal.es.channel.TotoroChannel;
import com.totoro.canal.es.consum.es.ElasticSearchLoadTask;
import com.totoro.canal.es.select.selector.TotoroSelector;
import com.totoro.canal.es.select.selector.canal.CanalEmbedSelector;
import com.totoro.canal.es.transform.TransFormTask;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

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

    private TotoroChannel channel;

    private TotoroSelector totoroSelector;

    private ElasticSearchLoadTask elasticSearchLoadTask;

    private TransFormTask transFormTask;


    public CanalScheduler(final Properties conf) {
        this.conf = conf;
        channel = new TotoroChannel();
        totoroSelector = new CanalEmbedSelector();
        elasticSearchLoadTask = new ElasticSearchLoadTask(channel);
        transFormTask = new TransFormTask(channel);
    }

    public void start() throws InterruptedException, ExecutionException {
        //主线程所在
        running = true;
        totoroSelector.start();
        totoroSelector.rollback();


        transFormTask.start();
        elasticSearchLoadTask.start();

        while (running) {

            Message message = totoroSelector.selector();

            long batchId = message.getId();
            int size = message.getEntries().size();
            if (batchId == -1 || size == 0) {
                System.out.println("空数据");
            } else {
                System.out.println("放入数据");
                channel.putMessage(message);
                totoroSelector.ack(batchId); // 提交确认
            }
        }
    }

    public void stop() {
        totoroSelector.stop();
        elasticSearchLoadTask.stop();
    }


}
