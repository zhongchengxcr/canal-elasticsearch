package com.totoro.canal.es.common;

import com.alibaba.otter.canal.common.utils.BooleanMutex;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/12/05 上午10:04
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class RollBackMonitorFactory {

    private static BooleanMutex booleanMutex = new BooleanMutex(false);

    public static BooleanMutex getBooleanMutex() {
        return booleanMutex;
    }
}
