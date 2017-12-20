package com.totoro.canal.es.consum.es;

import java.util.List;

/**
 * @author zhongcheng
 */
public interface ElasticsearchService {

    void insertById(String index, String type, List<ElasticsearchMetadata.EsRowData> esRowDataList);

    void update(String index, String type, List<ElasticsearchMetadata.EsRowData> esRowDataList);

    void deleteById(String index, String type, List<ElasticsearchMetadata.EsRowData> esRowDataList);

    void close();
}
