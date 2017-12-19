package com.totoro.canal.es.transform;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.totoro.canal.es.select.selector.CanalConf;
import org.apache.commons.lang.StringUtils;

import java.util.HashSet;
import java.util.Set;

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

    private static final String DELIMITER = ",";

    private static final String CONNECTOR = "\\.";

    private Set<String> acceptTable = new HashSet<>();

    public TableFilter(CanalConf canalConf) {
        String accept = canalConf.getAccept();
        String[] acceptArr = accept.split(DELIMITER);
        for (String str : acceptArr) {
            String[] strArr = str.split(CONNECTOR);
            if (strArr.length == 2) {
                acceptTable.add(str);
            } else if (strArr.length == 3) {
                acceptTable.add(StringUtils.substringBeforeLast(str, CONNECTOR));
            }
        }
    }


    @Override
    public boolean filter(CanalEntry.Entry entry) {
        String database = entry.getHeader().getSchemaName();
        String table = entry.getHeader().getTableName();
        return acceptTable.contains(database + CONNECTOR + table);
    }


}
