package com.totoro.canal.es.transform;

import com.totoro.canal.es.common.RecycleAble;

/**
 *
 * @param <I>
 * @param <O>
 * @author zhongcheng
 */
public interface TransForm<I, O> extends RecycleAble {

    O trans(I input);



}
