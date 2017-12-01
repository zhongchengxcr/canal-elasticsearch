package com.totoro.canal.es.consum.es;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.totoro.canal.es.channel.TotoroChannel;
import com.totoro.canal.es.common.AbstractTotoroLifeCycle;

import java.util.concurrent.*;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/12/01 下午12:49
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class ElasticSearchLoadTask extends AbstractTotoroLifeCycle {

    private ElasticSearchLoad elasticSearchLoad;

    private ExecutorService executorService;


    public ElasticSearchLoadTask(TotoroChannel totoroChannel) {
        elasticSearchLoad = new ElasticSearchLoad(totoroChannel);

        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("esload-pool-%d").build();

        executorService = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), threadFactory);

        executorService = Executors.newSingleThreadExecutor();

    }

    @Override
    public void start() {
        super.start();
        executorService.submit(elasticSearchLoad);
    }

    @Override
    public void stop() {
        super.stop();
        elasticSearchLoad.stop();

    }
}
