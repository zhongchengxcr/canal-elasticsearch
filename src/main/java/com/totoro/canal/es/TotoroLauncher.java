package com.totoro.canal.es;

import com.google.common.io.CharStreams;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
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


    static {
            System.out.println("");
            System.out.println(" _____       _");
            System.out.println("|_   _|___  | |_  ___   _ __  ___  ");
            System.out.println("  | | / _ \\ | __|/ _ \\ | '__|/ _ \\ ");
            System.out.println("  | || (_) || |_| (_) || |  | (_) |");
            System.out.println("  |_| \\___/  \\__|\\___/ |_|   \\___/");
            System.out.println("[Totoro 1.0-SNAPSHOT,Build 2017/12/20,Author:zhongcheng_m@yeah.net]");
            System.out.println("");
    }

    public static void main(String[] args) throws InterruptedException, IOException, ExecutionException {

        setGlobalUncaughtExceptionHandler();

        String conf = System.getProperty("canal-es.properties", "classpath:canal-es.properties");

        Properties properties = getProperties(conf);

        TotoroBootStrap canalScheduler = new TotoroBootStrap(properties);
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
