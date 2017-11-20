package com.totoro.canal.es.consum.es.impl;


import com.totoro.canal.es.consum.es.ElasticsearchService;

import java.util.Map;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/11/19 下午6:53
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class ElasticsearchServiceImpl implements ElasticsearchService {
    @Override
    public void insertById(String index, String type, String id, Map<String, Object> dataMap) {

    }

    @Override
    public void batchInsertById(String index, String type, Map<String, Map<String, Object>> idDataMap) {

    }

    @Override
    public void update(String index, String type, String id, Map<String, Object> dataMap) {

    }

    @Override
    public void deleteById(String index, String type, String id) {

    }
}
