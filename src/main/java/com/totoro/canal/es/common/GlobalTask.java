
package com.totoro.canal.es.common;

import org.apache.commons.lang.ClassUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * mainstem,select,extract,transform,load parent Thread.
 *
 * @author xiaoqing.zhouxq 2011-8-23 上午10:38:14
 */
public abstract class GlobalTask extends Thread {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    protected volatile boolean running = false;

    protected ExecutorService executorService;

    protected Map<Long, Future> pendingFuture;


    public GlobalTask() {
        setName(createTaskName(ClassUtils.getShortClassName(this.getClass())));
        pendingFuture = new HashMap<>();
    }

    public void shutdown() {
        running = false;
        interrupt();
        List<Future> cancelFutures = new ArrayList<>();
        for (Map.Entry<Long, Future> entry : pendingFuture.entrySet()) {
            if (!entry.getValue().isDone()) {
                logger.warn("WARN ## Task future processId[{}] canceled!", entry.getKey());
                cancelFutures.add(entry.getValue());
            }
        }

        for (Future future : cancelFutures) {
            future.cancel(true);
        }
        pendingFuture.clear();

        if (executorService != null) {
            executorService.shutdown();
        }
    }


    protected String createTaskName(String taskName) {
        return new StringBuilder().append("taskName = ").append(taskName).toString();
    }


    protected boolean isInterrupt(Throwable e) {
        if (!running) {
            return true;
        }

        if (ExceptionUtils.getRootCause(e) instanceof InterruptedException) {
            return true;
        }

        return false;

    }

    // ====================== setter / getter =========================

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }


}
