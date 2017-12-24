package com.totoro.canal.es.consum.es;

import com.google.common.base.Joiner;
import com.totoro.canal.es.common.RecycleAble;
import com.totoro.canal.es.transform.TransForm;
import io.netty.util.Recycler;

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
public class ElasticsearchMetadata implements RecycleAble {

    public final static int INSERT = 1;

    public final static int DELETE = 2;

    public final static int UPDATE = 3;

    private Long batchId;

    private EsEntryArrayList esEntries;

    private final Recycler.Handle<ElasticsearchMetadata> handle;

    private TransForm transForm;

    public ElasticsearchMetadata(Recycler.Handle<ElasticsearchMetadata> handle) {
        this.handle = handle;
    }

    @Override
    public boolean recycle() {
        batchId = null;
        if (esEntries != null) {
            esEntries.recycle();
        }

        if(transForm!=null){
            transForm.recycle();
        }

        handle.recycle(this);
        return true;
    }

    public static class EsEntry implements RecycleAble {

        private String index;

        private String type;

        private int eventType;

        private EsRowDataArrayList esRowDatas;

        private final Recycler.Handle<EsEntry> handle;

        @Override
        public boolean recycle() {
            index = null;
            type = null;
            eventType = 0;
            if (esRowDatas != null) {
                esRowDatas.recycle();
            }
            handle.recycle(this);
            return true;
        }


        public EsEntry(Recycler.Handle<EsEntry> handle) {
            this.handle = handle;
        }

        public String getIndex() {
            return index;
        }

        public EsEntry setIndex(String index) {
            this.index = index;
            return this;
        }

        public String getType() {
            return type;
        }

        public EsEntry setType(String type) {
            this.type = type;
            return this;
        }

        public EsRowDataArrayList getEsRowDatas() {
            return esRowDatas;
        }

        public EsEntry setEsRowDatas(EsRowDataArrayList esRowDatas) {
            this.esRowDatas = esRowDatas;
            return this;
        }

        public int getEventType() {
            return eventType;
        }

        public EsEntry setEventType(int eventType) {
            this.eventType = eventType;
            return this;
        }


        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("EsEntry{");
            sb.append("index='").append(index).append('\'');
            sb.append(", type='").append(type).append('\'');
            sb.append(", eventType=").append(getEventTypeName(eventType));
            sb.append(", esRowDatas=").append(Joiner.on(",").join(esRowDatas));
            sb.append('}');
            return sb.toString();
        }


        public static String getEventTypeName(int eventType) {
            if (eventType == INSERT) {
                return "INSERT";
            } else if (eventType == DELETE) {
                return "DELETE";
            } else if (eventType == UPDATE) {
                return "UPDATE";
            } else {
                return "UNKNOW_TYPE";
            }
        }


    }

    public static class EsRowData implements RecycleAble {
        public String idColumn;

        public EsColumnHashMap rowData;

        private final Recycler.Handle<EsRowData> handle;

        public EsRowData(Recycler.Handle<EsRowData> handle) {
            this.handle = handle;
        }

        @Override
        public boolean recycle() {
            idColumn = null;

            if(rowData!=null){
                rowData.recycle();
            }
            handle.recycle(this);
            return true;
        }


        public String getIdColumn() {
            return idColumn;
        }

        public EsRowData setIdColumn(String idColumn) {
            this.idColumn = idColumn;
            return this;
        }

        public Map<String, Object> getRowData() {
            return rowData;
        }

        public EsRowData setRowData(EsColumnHashMap rowData) {
            this.rowData = rowData;
            return this;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("EsRowData{");
            sb.append("idColumn='").append(idColumn).append('\'');
            sb.append(", rowData=").append(rowData);
            sb.append('}');
            return sb.toString();
        }


    }


    public Long getBatchId() {
        return batchId;
    }

    public ElasticsearchMetadata setBatchId(Long batchId) {
        this.batchId = batchId;
        return this;
    }


    public EsEntryArrayList getEsEntries() {
        return esEntries;
    }

    public ElasticsearchMetadata setEsEntries(EsEntryArrayList esEntries) {
        this.esEntries = esEntries;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ElasticsearchMetadata{");
        sb.append("batchId=").append(batchId);
        sb.append(", esEntries=").append(Joiner.on(",").join(esEntries));
        sb.append('}');
        return sb.toString();
    }

    public ElasticsearchMetadata setTransForm(TransForm transForm) {
        this.transForm = transForm;
        return this;
    }

    public TransForm getTransForm() {
        return transForm;
    }
}
