package com.totoro.canal.es.transform;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.totoro.canal.es.common.TotoroObjectPool;
import com.totoro.canal.es.consum.es.ElasticsearchMetadata;
import com.totoro.canal.es.consum.es.EsColumnHashMap;
import com.totoro.canal.es.consum.es.EsEntryArrayList;
import com.totoro.canal.es.consum.es.EsRowDataArrayList;
import io.netty.util.Recycler;
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

    public TotoroTransForm(Recycler.Handle<TransForm> handle) {
        this.handle = handle;
    }

    private final Recycler.Handle<TransForm> handle;


    @Override
    public boolean recycle() {
        esAdapter = null;
        message = null;
        handle.recycle(this);
        return true;
    }

    @Override
    public ElasticsearchMetadata call() throws Exception {
        return trans(message);
    }

    @Override
    public ElasticsearchMetadata trans(Message message) {
        List<CanalEntry.Entry> entries = message.getEntries();
        ElasticsearchMetadata elasticsearchMetadata = TotoroObjectPool.esMetadata();
        elasticsearchMetadata.setBatchId(message.getId());
        if (entries != null && entries.size() > 0) {
            EsEntryArrayList esEntryList = TotoroObjectPool.esEntryArrayList();

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
            elasticsearchMetadata.setEsEntries(esEntryList);
        }

        logger.info("Trans form complete message id =====> {}", message.getId());
        logger.info("Trans form complete elasticsearch metadata  =====> {}", elasticsearchMetadata.toString());
        return elasticsearchMetadata;
    }

    private ElasticsearchMetadata.EsEntry getElasticsearchMetadata(CanalEntry.Entry entry) throws InvalidProtocolBufferException {

        final String database = entry.getHeader().getSchemaName(); // => index
        final String table = entry.getHeader().getTableName();// => type
        final CanalEntry.RowChange change = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        final List<CanalEntry.RowData> rowDataList = change.getRowDatasList();

        CanalEntry.EventType eventType = entry.getHeader().getEventType();
        final int esEventType = esAdapter.getEsEventType(eventType);


        EsRowDataArrayList esRowDataList = TotoroObjectPool.esRowDataArrayList();

        for(CanalEntry.RowData rowData:rowDataList){
            List<CanalEntry.Column> columnList = esAdapter.getColumnList(esEventType, rowData);
            ElasticsearchMetadata.EsRowData esRowData = TotoroObjectPool.esRowData();
            EsColumnHashMap columnMap = TotoroObjectPool.esColumnHashMap();
            columnList.forEach(column -> columnMap.put(column.getName(), column.getValue()));
            esRowData.setRowData(columnMap);
            esRowData.setIdColumn(esAdapter.getEsIdColumn(database, table));//获取es对应的id Column
            esRowDataList.add(esRowData);
        }

        ElasticsearchMetadata.EsEntry esEntry = TotoroObjectPool.esEntry();

        esEntry.setIndex(database)
                .setType(table)
                .setEsRowDatas(esRowDataList)
                .setEventType(esEventType);

        return esEntry;
    }


    public TotoroTransForm setMessage(Message message) {
        this.message = message;
        return this;
    }

    public TotoroTransForm setEsAdapter(EsAdapter esAdapter) {
        this.esAdapter = esAdapter;
        return this;
    }
}
