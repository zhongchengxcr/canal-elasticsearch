package com.totoro.canal.es.consum.es;

import com.google.common.base.Joiner;

import java.util.List;
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

    public final static int INSERT = 1;

    public final static int DELETE = 2;

    public final static int UPDATE = 3;

    private Long batchId;

    private List<EsEntry> esEntries;

    public static class EsEntry {

        private String index;

        private String type;

        private int eventType;

        private List<EsRowData> esRowDatas;

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

        public List<EsRowData> getEsRowDatas() {
            return esRowDatas;
        }

        public EsEntry setEsRowDatas(List<EsRowData> esRowDatas) {
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

    public static class EsRowData {
        public String idColumn;

        public Map<String, Object> rowData;


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

        public EsRowData setRowData(Map<String, Object> rowData) {
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

    public List<EsEntry> getEsEntries() {
        return esEntries;
    }

    public ElasticsearchMetadata setEsEntries(List<EsEntry> esEntries) {
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
}
