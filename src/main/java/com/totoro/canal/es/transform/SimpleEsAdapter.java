package com.totoro.canal.es.transform;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.totoro.canal.es.consum.es.ElasticsearchMetadata;
import com.totoro.canal.es.select.selector.CanalConf;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private Logger logger = LoggerFactory.getLogger(getClass());

    private static Map<CanalEntry.EventType, Integer> eventTypePair = new ConcurrentHashMap<>();

    private static final String DELIMITER = ",";

    private static final String CONNECTOR = "\\.";

    private static final String CONNECTOR_TEP = ".";

    /**
     * ConcurrentHashMap其实可以考虑换成普通 hasmap
     * 因为正常情况下不会出现并发写 导致的扩容死循环问题
     * 但考虑到以后可能会增加功能而导致并发，所以选择ConcurrentHashMap
     * 并且目前大部分是读操作，性能不会相差太多
     */
    private static Map<String, String> idPair = new ConcurrentHashMap<>();

    public SimpleEsAdapter(CanalConf canalConf) {

        String accept = canalConf.getAccept();
        String[] acceptArr = accept.split(DELIMITER);
        for (String str : acceptArr) {
            String[] strArr = str.split(CONNECTOR);
            if (strArr.length == 3) {
                String dataBaseTable = StringUtils.substringBeforeLast(str, CONNECTOR_TEP);
                String idColumn = StringUtils.substringAfterLast(str, CONNECTOR_TEP);
                idPair.put(dataBaseTable, idColumn);

                logger.info("Add accept :{}", str);
            }
        }

    }

    static {
        eventTypePair.put(CanalEntry.EventType.INSERT, ElasticsearchMetadata.INSERT);
        eventTypePair.put(CanalEntry.EventType.UPDATE, ElasticsearchMetadata.UPDATE);
        eventTypePair.put(CanalEntry.EventType.DELETE, ElasticsearchMetadata.DELETE);
    }


    @Override
    public String getEsIdColumn(String database, String table) {
        return idPair.get(database + CONNECTOR_TEP + table);
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
