package com.totoro.canal.es.transform;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.List;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/12/19 上午9:49
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public interface EsAdapter {

    String getEsIdColumn(String database, String table);

    int getEsEventType(CanalEntry.EventType eventType);

    List<CanalEntry.Column> getColumnList(int esEventType, CanalEntry.RowData rowData);
}
