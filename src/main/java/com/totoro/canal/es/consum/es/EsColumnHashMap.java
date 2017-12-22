package com.totoro.canal.es.consum.es;

import com.totoro.canal.es.common.RecycleAble;
import io.netty.util.Recycler;

import java.util.HashMap;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/12/22 下午1:00
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class EsColumnHashMap extends HashMap<String, Object> implements RecycleAble {


    private final Recycler.Handle<EsColumnHashMap> handle;


    public EsColumnHashMap(Recycler.Handle<EsColumnHashMap> handle) {
        this.handle = handle;
    }


    @Override
    public boolean recycle() {
        this.clear();
        handle.recycle(this);
        return true;
    }
}
