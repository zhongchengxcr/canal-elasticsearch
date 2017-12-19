package com.totoro.canal.es.transform;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.HashSet;
import java.util.Set;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/12/18 下午3:31
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class SimpleMessageFilter implements MessageFilter {

    private static final Set<CanalEntry.EventType> acceptEventType = new HashSet<>();


    static {
        acceptEventType.add(CanalEntry.EventType.INSERT);
        acceptEventType.add(CanalEntry.EventType.DELETE);
        acceptEventType.add(CanalEntry.EventType.UPDATE);
    }

    @Override
    public boolean filter(CanalEntry.Entry entry) {

        //过滤掉事物头尾等 非 row data 的 entry
        boolean rowData = entry.getEntryType() == CanalEntry.EntryType.ROWDATA;

        if (!rowData) {
            return false;
        }

        //只保存 insert update delete 类型的 时间
        boolean eventType = filterEventType(entry.getHeader().getEventType());

        if (!eventType) {
            return false;
        }

        return true;
    }


    private boolean filterEventType(CanalEntry.EventType eventType) {
        return acceptEventType.contains(eventType);
    }
}