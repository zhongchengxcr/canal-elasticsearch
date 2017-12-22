package com.totoro.canal.es.consum.es;

import com.totoro.canal.es.common.RecycleAble;
import io.netty.util.Recycler;

import java.util.ArrayList;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/12/22 下午2:21
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class EsEntryArrayList extends ArrayList<ElasticsearchMetadata.EsEntry> implements RecycleAble {

    private final Recycler.Handle<EsEntryArrayList> handle;


    public EsEntryArrayList(Recycler.Handle<EsEntryArrayList> handle) {
        this.handle = handle;
    }

    @Override
    public boolean recycle() {

        if (this.size() > 0) {
            this.forEach(ElasticsearchMetadata.EsEntry::recycle);
        }

        this.clear();
        handle.recycle(this);
        return true;
    }

}
