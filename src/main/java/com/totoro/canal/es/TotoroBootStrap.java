package com.totoro.canal.es;

import com.totoro.canal.es.channel.TotoroChannel;
import com.totoro.canal.es.common.GlobalTask;
import com.totoro.canal.es.consum.es.*;
import com.totoro.canal.es.consum.es.impl.ElasticsearchServiceImpl;
import com.totoro.canal.es.select.selector.CanalConf;
import com.totoro.canal.es.select.selector.SelectorTask;
import com.totoro.canal.es.select.selector.TotoroSelector;
import com.totoro.canal.es.select.selector.canal.CanalEmbedSelector;
import com.totoro.canal.es.transform.*;
import org.apache.commons.lang.ClassUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
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
public class TotoroBootStrap {


    private static final Logger logger = LoggerFactory.getLogger(TotoroLauncher.class);

    private volatile boolean running = false;

    private TotoroChannel channel;

    private TotoroSelector totoroSelector;

    private ElasticsearchService elasticsearchService;

    private Map<String, GlobalTask> taskMap = new ConcurrentHashMap<>();

    public TotoroBootStrap(final Properties conf) throws UnknownHostException {

        CanalConf canalConf = getCanalConf(conf);
        EsConf esConf = getEsConf(conf);
        int transThreadNum = getTransThreadNum(conf);
        //totoroSelector需要提前初始化好
        totoroSelector = new CanalEmbedSelector(canalConf);
        //初始化 Channel
        initChannel();
        //初始化 SelectorTask
        SelectorTask selectorTask = initSelectorTask();
        //初始化 TransFormTask
        TransFormTask transFormTask = initTransFormTask(canalConf, transThreadNum);
        //初始化 ConsumerTask
        ConsumerTask consumerTask = initConsumerTask(esConf);

        taskMap.put(ClassUtils.getShortClassName(SelectorTask.class), selectorTask);
        taskMap.put(ClassUtils.getShortClassName(TransFormTask.class), transFormTask);
        taskMap.put(ClassUtils.getShortClassName(ConsumerTask.class), consumerTask);


        logger.info("Totoro init complete .......");
    }


    private SelectorTask initSelectorTask() {
        return new SelectorTask(totoroSelector, channel, this);
    }

    private TransFormTask initTransFormTask(CanalConf canalConf, int transThreadNum) {
        MessageFilter tableFilter = new TableFilter(canalConf);
        MessageFilter simpleFilter = new SimpleMessageFilter();

        MessageFilterChain messageFilterChain = MessageFilterChain.getInstance();
        messageFilterChain.register(tableFilter);
        messageFilterChain.register(simpleFilter);

        EsAdapter esAdapter = new SimpleEsAdapter(canalConf);

        return new TransFormTask(channel, esAdapter, transThreadNum);
    }

    private ConsumerTask initConsumerTask(EsConf esConf) throws UnknownHostException {

        elasticsearchService = new ElasticsearchServiceImpl(esConf);
        Consumer consumer = new ElasticSearchConsumer(elasticsearchService);
        ConsumerTask consumerTask = new ConsumerTask(channel);
        consumerTask.register(consumer);
        return consumerTask;
    }


    private void initChannel() {
        channel = new TotoroChannel(totoroSelector);
    }


    public void start() throws InterruptedException, ExecutionException {
        //主线程所在
        running = true;
        taskMap.forEach((key, value) -> value.start());
    }

    public void stop() {
        taskMap.forEach((key, value) -> value.shutdown());
        taskMap.clear();
        channel.close();
        totoroSelector.stop();
        elasticsearchService.close();
    }


    private EsConf getEsConf(Properties conf) {

        String address = conf.getProperty("totoro.es.address");
        String clusterName = conf.getProperty("totoro.es.cluster.name");
        String username = conf.getProperty("totoro.es.username");
        String password = conf.getProperty("totoro.es.password");

        return new EsConf()
                .setAddress(address)
                .setClusterName(clusterName)
                .setUsername(username)
                .setPassword(password)
                .builder();
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


    private int getTransThreadNum(Properties conf) {
        String numStr = conf.getProperty("totoro.cannal.trans.thread.nums");

        if (numStr == null || StringUtils.isEmpty(numStr.trim())) {
            return 3;
        }
        return Integer.valueOf(numStr);
    }


    public Map<String, GlobalTask> getTaskMap() {
        return taskMap;
    }


}
