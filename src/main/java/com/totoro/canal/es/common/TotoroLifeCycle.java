package com.totoro.canal.es.common;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/11/19 下午6:51
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public interface TotoroLifeCycle {

    /**
     * 开始
     */
    void start();

    /**
     * 结束
     */
    void stop();

    /**
     * 是否开始
     *
     * @return
     */
    boolean isStart();
}
