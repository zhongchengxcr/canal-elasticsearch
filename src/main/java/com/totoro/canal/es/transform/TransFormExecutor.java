package com.totoro.canal.es.transform;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.totoro.canal.es.model.es.ElasticsearchMetadata;

import java.util.concurrent.*;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/12/01 下午1:18
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class TransFormExecutor {

    private ThreadPoolExecutor threadPoolExecutor;


    public TransFormExecutor() {

        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("trans-pool-%d").build();

        threadPoolExecutor = new ThreadPoolExecutor(10
                , 10
                , 0L
                , TimeUnit.SECONDS
                , new LinkedBlockingQueue<>(50)
                , threadFactory);
    }


    public Future<ElasticsearchMetadata> submit(TotoroTransForm transForm) {
        return threadPoolExecutor.submit(transForm);
    }
}
