/*
 * Copyright (C) 2010-2101 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.totoro.canal.es.select.selector;

import com.alibaba.otter.canal.protocol.CanalEntry.Column;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.totoro.canal.es.model.EventData;
import com.totoro.canal.es.select.exception.SelectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 标题、简要说明. <br>
 * 类详细说明.
 * <p>
 * Copyright: Copyright (c)
 * <p>
 * Company: xx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class MessageParser {

    private static final Logger logger = LoggerFactory.getLogger(MessageParser.class);

    /**
     * 将对应canal送出来的Entry对象解析为totoro使用的内部对象
     * <p>
     * <pre>
     * 需要处理数据过滤：
     * 1. Transaction Begin/End过滤
     * </pre>
     */
    public List<EventData> parse(Long pipelineId, List<Entry> datas) throws SelectException {
        List<EventData> eventDatas = new ArrayList<EventData>();
        List<Entry> transactionDataBuffer = new ArrayList<Entry>();

        long now = new Date().getTime();
        try {
            for (Entry entry : datas) {
                switch (entry.getEntryType()) {
                    case TRANSACTIONBEGIN:
                        break;
                    case ROWDATA:
                        String tableName = entry.getHeader().getTableName();

                        RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());

                        break;
                    case TRANSACTIONEND:

                        break;
                    default:
                        break;
                }
            }

        } catch (Exception e) {
            throw new SelectException(e);
        }

        return eventDatas;
    }


    private Column getColumnIgnoreCase(List<Column> columns, String columName) {
        for (Column column : columns) {
            if (column.getName().equalsIgnoreCase(columName)) {
                return column;
            }
        }

        return null;
    }


}
