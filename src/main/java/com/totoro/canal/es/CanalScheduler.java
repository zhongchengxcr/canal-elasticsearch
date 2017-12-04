package com.totoro.canal.es;

import com.totoro.canal.es.channel.TotoroChannel;
import com.totoro.canal.es.consum.es.Consumer;
import com.totoro.canal.es.consum.es.ConsumerTask;
import com.totoro.canal.es.consum.es.ElasticSearchLoad;
import com.totoro.canal.es.consum.es.ElasticsearchService;
import com.totoro.canal.es.consum.es.impl.ElasticsearchServiceImpl;
import com.totoro.canal.es.select.selector.SelectorTask;
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

    private ConsumerTask consumerTask;

    private TransFormTask transFormTask;

    private SelectorTask selectorTask;


    public CanalScheduler(final Properties conf) {
        this.conf = conf;
        channel = new TotoroChannel();
        totoroSelector = new CanalEmbedSelector();

        ElasticsearchService elasticsearchService = new ElasticsearchServiceImpl();
        Consumer consumer = new ElasticSearchLoad(elasticsearchService);

        consumerTask = new ConsumerTask(channel, consumer);
        transFormTask = new TransFormTask(channel);
        selectorTask = new SelectorTask(totoroSelector, channel);
    }

    public void start() throws InterruptedException, ExecutionException {
        //主线程所在
        running = true;
        selectorTask.start();
        transFormTask.start();
        consumerTask.start();

    }

    public void stop() {
        totoroSelector.stop();
        consumerTask.shutdown();
        transFormTask.shutdown();
        selectorTask.shutdown();
    }


}
