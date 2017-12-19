package com.totoro.canal.es.transform;

import com.alibaba.otter.canal.protocol.CanalEntry;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/12/18 下午3:29
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class TableFilter implements MessageFilter {

    public TableFilter(MessageFilterChain messageFilterChain) {
        messageFilterChain.register(this);
    }

    public TableFilter() {
    }


    @Override
    public boolean filter(CanalEntry.Entry entry) {
        return true;
    }


}
