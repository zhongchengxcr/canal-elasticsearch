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

}
