package com.totoro.canal.es;

import com.alibaba.otter.canal.protocol.CanalEntry;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * <p>
 * Copyright: Copyright (c)
 * <p>
 * Company: xx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class TotoroLauncher {

    private static final Logger logger = LoggerFactory.getLogger(TotoroLauncher.class);

    private static final String CLASSPATH_URL_PREFIX = "classpath:";

    public static void main(String[] args) throws InterruptedException, IOException, ExecutionException {

        setGlobalUncaughtExceptionHandler();

        String conf = System.getProperty("canal-es.properties", "classpath:canal-es.properties");

        Properties properties = getProperties(conf);

        CanalScheduler canalScheduler = new CanalScheduler(properties);
        canalScheduler.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logger.info("## stop the totoro server");
                canalScheduler.stop();
            } catch (Throwable e) {
                logger.warn("##something goes wrong when stopping totoro Server:", e);
            } finally {
                logger.info("## totoro server is down.");
            }
        }));

    }

    private static Properties getProperties(String conf) throws IOException {
        Properties properties = new Properties();

        if (conf.startsWith(CLASSPATH_URL_PREFIX)) {
            conf = StringUtils.substringAfter(conf, CLASSPATH_URL_PREFIX);
            logger.info(conf);
            properties.load(TotoroLauncher.class.getClassLoader().getResourceAsStream(conf));
        } else {
            properties.load(new FileInputStream(conf));
        }
        return properties;
    }


    private static void setGlobalUncaughtExceptionHandler() {
        Thread.setDefaultUncaughtExceptionHandler((t, e) -> logger.error("UnCaughtException", e));
    }


    private static void printEntry(List<CanalEntry.Entry> entrys) {

        for (CanalEntry.Entry entry : entrys) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChage = null;
            try {
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            CanalEntry.EventType eventType = rowChage.getEventType();
            System.out.println(String.format("================>; binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));

            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == CanalEntry.EventType.DELETE) {
                    printColumn(rowData.getBeforeColumnsList());
                } else if (eventType == CanalEntry.EventType.INSERT) {
                    printColumn(rowData.getAfterColumnsList());
                } else {
                    logger.info("------->; before");
                    printColumn(rowData.getBeforeColumnsList());
                    logger.info("------->; after");
                    printColumn(rowData.getAfterColumnsList());
                }
            }
        }
    }

    private static void printColumn(List<CanalEntry.Column> columns) {
        for (CanalEntry.Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }
}
