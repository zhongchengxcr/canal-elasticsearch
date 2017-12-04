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
public class ElasticSearchLoad implements Consumer<ElasticsearchMetadata> {


    private ElasticsearchService elasticsearchService;

    public ElasticSearchLoad(ElasticsearchService elasticsearchService) {
        this.elasticsearchService = elasticsearchService;
    }


    @Override
    public void consume(ElasticsearchMetadata object) {
        //elasticsearchService.deleteById();
        System.out.println("consum......");
    }
}
