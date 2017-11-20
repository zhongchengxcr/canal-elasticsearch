package com.totoro.canal.es.consum.es;

import java.util.Map;

/**
 * @author zhongcheng
 */
public interface ElasticsearchService {

    void insertById(String index, String type, String id, Map<String, Object> dataMap);

    void batchInsertById(String index, String type, Map<String, Map<String, Object>> idDataMap);

    void update(String index, String type, String id, Map<String, Object> dataMap);

    void deleteById(String index, String type, String id);
}
