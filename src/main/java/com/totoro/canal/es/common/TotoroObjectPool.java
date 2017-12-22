package com.totoro.canal.es.common;

import com.totoro.canal.es.consum.es.ElasticsearchMetadata;
import com.totoro.canal.es.consum.es.EsColumnHashMap;
import com.totoro.canal.es.consum.es.EsEntryArrayList;
import com.totoro.canal.es.consum.es.EsRowDataArrayList;
import com.totoro.canal.es.transform.TotoroTransForm;
import com.totoro.canal.es.transform.TransForm;
import io.netty.util.Recycler;

import java.util.HashMap;
import java.util.Map;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/12/22 下午12:49
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class TotoroObjectPool {


    public static ElasticsearchMetadata esMetadata() {
        return ELASTICSEARCH_METADATA_RECYCLER.get();
    }

    public static ElasticsearchMetadata.EsEntry esEntry() {
        return ELASTICSEARCH_ENTRY_RECYCLER.get();
    }

    public static ElasticsearchMetadata.EsRowData esRowData() {
        return ELASTICSEARCH_ROWDATA_RECYCLER.get();
    }

    public static EsColumnHashMap esColumnHashMap() {
        return ELASTICSEARCH_COLUMNMAP_RECYCLER.get();
    }


    public static EsEntryArrayList esEntryArrayList() {
        return ELASTICSEARCH_ENTRY_LIST__RECYCLER.get();
    }

    public static EsRowDataArrayList esRowDataArrayList() {
        return ELASTICSEARCH_ROW_DATA__RECYCLER.get();
    }


    public static TransForm transForm() {
        return TRANS_FORM_RECYCLER.get();
    }


    private static final Recycler<ElasticsearchMetadata> ELASTICSEARCH_METADATA_RECYCLER =
            new Recycler<ElasticsearchMetadata>() {
                @Override
                protected ElasticsearchMetadata newObject(Handle<ElasticsearchMetadata> handle) {
                    return new ElasticsearchMetadata(handle);
                }
            };


    private static final Recycler<ElasticsearchMetadata.EsEntry> ELASTICSEARCH_ENTRY_RECYCLER =
            new Recycler<ElasticsearchMetadata.EsEntry>() {
                @Override
                protected ElasticsearchMetadata.EsEntry newObject(Handle<ElasticsearchMetadata.EsEntry> handle) {
                    return new ElasticsearchMetadata.EsEntry(handle);
                }
            };

    private static final Recycler<ElasticsearchMetadata.EsRowData> ELASTICSEARCH_ROWDATA_RECYCLER =
            new Recycler<ElasticsearchMetadata.EsRowData>() {
                @Override
                protected ElasticsearchMetadata.EsRowData newObject(Handle<ElasticsearchMetadata.EsRowData> handle) {
                    return new ElasticsearchMetadata.EsRowData(handle);
                }
            };


    private static final Recycler<EsColumnHashMap> ELASTICSEARCH_COLUMNMAP_RECYCLER =
            new Recycler<EsColumnHashMap>() {
                @Override
                protected EsColumnHashMap newObject(Handle<EsColumnHashMap> handle) {
                    return new EsColumnHashMap(handle);
                }
            };


    private static final Recycler<TransForm> TRANS_FORM_RECYCLER =
            new Recycler<TransForm>() {
                @Override
                protected TransForm newObject(Handle<TransForm> handle) {
                    return new TotoroTransForm(handle);
                }
            };

    private static final Recycler<EsEntryArrayList> ELASTICSEARCH_ENTRY_LIST__RECYCLER =
            new Recycler<EsEntryArrayList>() {
                @Override
                protected EsEntryArrayList newObject(Handle<EsEntryArrayList> handle) {
                    return new EsEntryArrayList(handle);
                }
            };

    private static final Recycler<EsRowDataArrayList> ELASTICSEARCH_ROW_DATA__RECYCLER =
            new Recycler<EsRowDataArrayList>() {
                @Override
                protected EsRowDataArrayList newObject(Handle<EsRowDataArrayList> handle) {
                    return new EsRowDataArrayList(handle);
                }
            };


}
