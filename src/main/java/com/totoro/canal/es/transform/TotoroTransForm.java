package com.totoro.canal.es.transform;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.totoro.canal.es.model.es.ElasticsearchMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/11/19 下午6:44
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class TotoroTransForm implements TransForm<Message, ElasticsearchMetadata>, Callable<ElasticsearchMetadata> {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private Message message;

    private MessageFilter messageFilterChain = MessageFilterChain.getInstance();

    private EsAdapter esAdapter;

    public TotoroTransForm(Message message, EsAdapter esAdapter) {
        this.message = message;
        this.esAdapter = esAdapter;
    }

    @Override
    public ElasticsearchMetadata call() throws Exception {
        return trans(message);
    }

    @Override
    public ElasticsearchMetadata trans(Message message) {
        logger.info(Thread.currentThread().getName() + "处理消息id ：" + message.getId());
        List<CanalEntry.Entry> entries = message.getEntries();
        ElasticsearchMetadata elasticsearchMetadata = null;
        if (entries != null && entries.size() > 0) {
            elasticsearchMetadata = new ElasticsearchMetadata();
            elasticsearchMetadata.setBatchId(message.getId());
            List<ElasticsearchMetadata.EsEntry> esEntryList = new ArrayList<>(entries.size());
            entries.forEach(entry -> {
                if (messageFilterChain.filter(entry)) {
                    try {
                        ElasticsearchMetadata.EsEntry esEntry = getElasticsearchMetadata(entry);
                        esEntryList.add(esEntry);
                    } catch (InvalidProtocolBufferException e) {
                        e.printStackTrace();
                    }
                }
            });

            if (esEntryList.size() <= 0) {
                return null;
            }
            elasticsearchMetadata.setEsEntries(esEntryList);
        }
        return elasticsearchMetadata;
    }


    private ElasticsearchMetadata.EsEntry getElasticsearchMetadata(CanalEntry.Entry entry) throws InvalidProtocolBufferException {

        final String database = entry.getHeader().getSchemaName(); // => index
        final String table = entry.getHeader().getTableName();// => type
        final CanalEntry.RowChange change = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        final List<CanalEntry.RowData> rowDataList = change.getRowDatasList();


        CanalEntry.EventType eventType = entry.getHeader().getEventType();
        final int esEventType = esAdapter.getEsEventType(eventType);


        List<ElasticsearchMetadata.EsRowData> esRowDataList = rowDataList.stream().map(rowData -> {

            List<CanalEntry.Column> columnList = esAdapter.getColumnList(esEventType, rowData);
            ElasticsearchMetadata.EsRowData esRowData = new ElasticsearchMetadata.EsRowData();
            Map<String, Object> columnMap = new HashMap<>(columnList.size());
            columnList.forEach(column -> columnMap.put(column.getName(), column.getValue()));
            esRowData.setRowData(columnMap);
            esRowData.setId(esAdapter.getEsIdColumn(database, table));//获取es对应的id

            return esRowData;

        }).collect(Collectors.toList());

        ElasticsearchMetadata.EsEntry esEntry = new ElasticsearchMetadata.EsEntry();

        esEntry.setIndex(database)
                .setType(table)
                .setEsRowDatas(esRowDataList)
                .setEventType(esEventType);

        return esEntry;
    }


}
