package com.totoro.canal.es.consum.es;

import java.util.List;

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
public class ElasticSearchConsumer implements Consumer {


    private ElasticsearchService elasticsearchService;

    public ElasticSearchConsumer(ElasticsearchService elasticsearchService) {
        this.elasticsearchService = elasticsearchService;
    }

    @Override
    public void consume(ElasticsearchMetadata metadata) {

        List<ElasticsearchMetadata.EsEntry> esEntries = metadata.getEsEntries();
        if (esEntries != null && esEntries.size() > 0) {

            esEntries.forEach(esEntry -> {
                int eventType = esEntry.getEventType();

                String index = esEntry.getIndex();
                String type = esEntry.getType();
                List<ElasticsearchMetadata.EsRowData> esRowDatas = esEntry.getEsRowDatas();

                if (ElasticsearchMetadata.INSERT == eventType) {
                    elasticsearchService.insertById(index, type, esRowDatas);
                } else if (ElasticsearchMetadata.DELETE == eventType) {
                    elasticsearchService.deleteById(index, type, esRowDatas);
                } else if (ElasticsearchMetadata.UPDATE == eventType) {
                    elasticsearchService.update(index, type, esRowDatas);
                }

            });
        }
    }


}
