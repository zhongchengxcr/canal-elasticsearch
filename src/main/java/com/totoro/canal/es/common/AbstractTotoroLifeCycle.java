package com.totoro.canal.es.common;

import com.alibaba.otter.canal.common.CanalLifeCycle;

/**
 * 基本实现 . <br>
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
public abstract class AbstractTotoroLifeCycle implements TotoroLifeCycle {

    protected volatile boolean running = false;

    @Override
    public boolean isStart() {
        return running;
    }

    @Override
    public void start() {
        if (running) {
            throw new TotoroException(this.getClass().getName() + " has startup , don't repeat start");
        }

        running = true;
    }

    @Override
    public void stop() {
        if (!running) {
            throw new TotoroException(this.getClass().getName() + " isn't start , please check");
        }
        running = false;
    }

}
