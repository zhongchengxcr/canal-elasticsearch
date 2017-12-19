package com.totoro.canal.es;

import com.totoro.canal.es.channel.TotoroChannel;
import com.totoro.canal.es.common.task.GlobalTask;
import com.totoro.canal.es.consum.es.Consumer;
import com.totoro.canal.es.consum.es.ConsumerTask;
import com.totoro.canal.es.consum.es.ElasticSearchLoad;
import com.totoro.canal.es.consum.es.ElasticsearchService;
import com.totoro.canal.es.consum.es.impl.ElasticsearchServiceImpl;
import com.totoro.canal.es.select.selector.CanalConf;
import com.totoro.canal.es.select.selector.SelectorTask;
import com.totoro.canal.es.select.selector.TotoroSelector;
import com.totoro.canal.es.select.selector.canal.CanalEmbedSelector;
import com.totoro.canal.es.transform.*;
import org.apache.commons.lang.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.net.util.IPAddressUtil;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
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


    private static final Logger logger = LoggerFactory.getLogger(TotoroLauncher.class);

    private Properties conf;

    private volatile boolean running = false;

    private TotoroChannel channel;

    private TotoroSelector totoroSelector;

    private Map<String, GlobalTask> taskMap = new ConcurrentHashMap<>();

    public CanalScheduler(final Properties conf) {


        logger.info("CanalScheduler init .......");

        this.conf = conf;
        CanalConf canalConf = getCanalConf(conf);
        totoroSelector = new CanalEmbedSelector(canalConf);
        channel = new TotoroChannel(totoroSelector);


        MessageFilterChain messageFilterChain = MessageFilterChain.getInstance();

        MessageFilter tableFilter = new TableFilter(canalConf);
        MessageFilter simpleFilter = new SimpleMessageFilter();

        messageFilterChain.register(tableFilter);
        messageFilterChain.register(simpleFilter);


        EsAdapter esAdapter = new SimpleEsAdapter(canalConf);

        ElasticsearchService elasticsearchService = new ElasticsearchServiceImpl();
        Consumer consumer = new ElasticSearchLoad(elasticsearchService);
        SelectorTask selectorTask = new SelectorTask(totoroSelector, channel, this);
        TransFormTask transFormTask = new TransFormTask(channel, esAdapter);
        ConsumerTask consumerTask = new ConsumerTask(channel, consumer);

        taskMap.put(ClassUtils.getShortClassName(SelectorTask.class), selectorTask);
        taskMap.put(ClassUtils.getShortClassName(TransFormTask.class), transFormTask);
        taskMap.put(ClassUtils.getShortClassName(ConsumerTask.class), consumerTask);


        logger.info("CanalScheduler init complete .......");
    }

    public void start() throws InterruptedException, ExecutionException {
        //主线程所在
        running = true;
        taskMap.forEach((key, value) -> value.start());
    }

    public void stop() {
        totoroSelector.stop();
        taskMap.forEach((key, value) -> value.shutdown());
        taskMap.clear();
    }


    private CanalConf getCanalConf(Properties conf) {

        String address = conf.getProperty("totoro.canal.address");
        String zkAddress = conf.getProperty("totoro.canal.zk.address");
        String username = conf.getProperty("totoro.canal.username");
        String password = conf.getProperty("totoro.canal.password");
        String mode = conf.getProperty("totoro.canal.mode");
        String destination = conf.getProperty("totoro.canal.destination");
        String filterPatten = conf.getProperty("totoro.canal.filter.patten");
        String accept = conf.getProperty("totoro.canal.table.accept");

        return new CanalConf()
                .setAddress(address)
                .setZkAddress(zkAddress)
                .setUserName(username)
                .setPassWord(password)
                .setMode(mode)
                .setDestination(destination)
                .setFilterPatten(filterPatten)
                .setAccept(accept)
                .builder();
    }


    public Map<String, GlobalTask> getTaskMap() {
        return taskMap;
    }


}
