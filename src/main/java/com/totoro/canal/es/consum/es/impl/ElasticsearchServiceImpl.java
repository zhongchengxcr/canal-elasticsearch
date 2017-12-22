package com.totoro.canal.es.consum.es.impl;


import com.totoro.canal.es.consum.es.ElasticsearchMetadata;
import com.totoro.canal.es.consum.es.ElasticsearchService;
import com.totoro.canal.es.consum.es.EsConf;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
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

    private Logger logger = LoggerFactory.getLogger(getClass());

    private TransportClient transportClient;


    public ElasticsearchServiceImpl(EsConf esConf) throws UnknownHostException {

        String clusterName = esConf.getClusterName();
        String address = esConf.getAddress();
        String[] hostPort = address.split(":");

        logger.info("Connect to elasticsearch  {}:{}", clusterName, address);

        Settings settings = Settings.builder().put("cluster.name", clusterName)
                .put("client.transport.sniff", true)
                .build();
        transportClient = new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostPort[0]), Integer.valueOf(hostPort[1])));


        logger.info("Complete the connection to elasticsearch");
    }

    @Override
    public void insertById(final String index, final String type, final List<ElasticsearchMetadata.EsRowData> esRowDataList) {
        esRowDataList.forEach(esRowData -> {
            String idColumn = esRowData.getIdColumn();
            Map<String, Object> dataMap = esRowData.getRowData();
            String id = (String) esRowData.getRowData().get(idColumn);
            transportClient.prepareIndex(index, type, id).setSource(dataMap).get();
            logger.info("Insert into elasticsearch  ====> {} ", index + "." + type + "." + id);
        });

    }

    @Override
    public void update(String index, String type, List<ElasticsearchMetadata.EsRowData> esRowDataList) {
        esRowDataList.forEach(esRowData -> {
            String idColumn = esRowData.getIdColumn();
            Map<String, Object> dataMap = esRowData.getRowData();
            String id = (String) esRowData.getRowData().get(idColumn);
            transportClient.prepareIndex(index, type, id).setSource(dataMap).get();
            logger.info("Update into elasticsearch  ====> {} ", index + "." + type + "." + id);
        });
    }

    @Override
    public void deleteById(String index, String type, List<ElasticsearchMetadata.EsRowData> esRowDataList) {
        esRowDataList.forEach(esRowData -> {
            String idColumn = esRowData.getIdColumn();
            String id = (String) esRowData.getRowData().get(idColumn);
            transportClient.prepareDelete(index, type, id).get();
            logger.info("Delete into elasticsearch  ====> {} ", index + "." + type + "." + id);

        });

    }

    @Override
    public void close() {
        transportClient.close();
    }
}
