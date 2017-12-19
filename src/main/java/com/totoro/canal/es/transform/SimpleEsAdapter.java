package com.totoro.canal.es.transform;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.totoro.canal.es.model.es.ElasticsearchMetadata;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/12/19 上午9:37
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class SimpleEsAdapter implements EsAdapter {

    private static Map<CanalEntry.EventType, Integer> eventTypePair = new ConcurrentHashMap<>();

    private static Map<String, String> idPair = new ConcurrentHashMap<>();


    static {
        eventTypePair.put(CanalEntry.EventType.INSERT, ElasticsearchMetadata.INSERT);
        eventTypePair.put(CanalEntry.EventType.UPDATE, ElasticsearchMetadata.UPDATE);
        eventTypePair.put(CanalEntry.EventType.DELETE, ElasticsearchMetadata.DELETE);
    }

    static {
        //TODO
    }

    @Override
    public String getEsIdColumn(String database, String table) {

        //TODO
        return "id";
    }

    @Override
    public int getEsEventType(CanalEntry.EventType eventType) {
        if (eventTypePair.containsKey(eventType)) {
            return eventTypePair.get(eventType);
        } else {
            throw new RuntimeException(); //一般情况下不会发生，因为在filter时已经做过判断
        }
    }

    @Override
    public List<CanalEntry.Column> getColumnList(int esEventType, CanalEntry.RowData rowData) {

        List<CanalEntry.Column> columnList;
        if (esEventType == ElasticsearchMetadata.DELETE) {
            columnList = rowData.getBeforeColumnsList();
        } else {
            columnList = rowData.getAfterColumnsList();
        }

        return columnList;

    }

}
