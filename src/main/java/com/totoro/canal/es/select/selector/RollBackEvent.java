package com.totoro.canal.es.select.selector;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/12/04 下午4:31
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class RollBackEvent {

    private long batchId;

    public RollBackEvent(long batchId) {
        this.batchId = batchId;
    }

    public long getBatchId() {
        return batchId;
    }

    public RollBackEvent setBatchId(long batchId) {
        this.batchId = batchId;
        return this;
    }
}
