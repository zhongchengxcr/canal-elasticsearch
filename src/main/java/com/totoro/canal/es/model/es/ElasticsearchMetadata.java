package com.totoro.canal.es.model.es;

import com.totoro.canal.es.common.Message;

import java.util.Map;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/11/19 下午6:32
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class ElasticsearchMetadata {
    protected Long batchId;

    private String index;

    private String type;

    private String id;

    private Map<String, Object> dataMap;

    private Map<String, Map<String, Object>> idDataMap;

    public String getIndex() {
        return index;
    }

    public ElasticsearchMetadata setIndex(String index) {
        this.index = index;
        return this;
    }

    public String getType() {
        return type;
    }

    public ElasticsearchMetadata setType(String type) {
        this.type = type;
        return this;
    }

    public String getId() {
        return id;
    }

    public ElasticsearchMetadata setId(String id) {
        this.id = id;
        return this;
    }

    public Map<String, Object> getDataMap() {
        return dataMap;
    }

    public ElasticsearchMetadata setDataMap(Map<String, Object> dataMap) {
        this.dataMap = dataMap;
        return this;
    }

    public Map<String, Map<String, Object>> getIdDataMap() {
        return idDataMap;
    }

    public ElasticsearchMetadata setIdDataMap(Map<String, Map<String, Object>> idDataMap) {
        this.idDataMap = idDataMap;
        return this;
    }

    public Long getBatchId() {
        return batchId;
    }

    public ElasticsearchMetadata setBatchId(Long batchId) {
        this.batchId = batchId;
        return this;
    }
}
