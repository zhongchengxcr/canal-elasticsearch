package com.totoro.canal.es.transform;

/**
 *
 * @param <I>
 * @param <O>
 * @author zhongcheng
 */
public interface TransForm<I, O> {

    O trans(I input);
}
