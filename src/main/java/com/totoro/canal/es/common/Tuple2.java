package com.totoro.canal.es.common;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/12/21 下午3:47
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class Tuple2<A, B> {

    public final A _1;

    public final B _2;


    private Tuple2(A _1, B _2) {
        this._1 = _1;
        this._2 = _2;
    }


    public static <A, B> Tuple2<A, B> apply(A _1, B _2) {
        return new Tuple2<>(_1, _2);
    }
}
