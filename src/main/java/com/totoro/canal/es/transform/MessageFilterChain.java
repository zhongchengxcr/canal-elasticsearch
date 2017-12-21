package com.totoro.canal.es.transform;

import com.alibaba.otter.canal.protocol.CanalEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/12/18 下午3:07
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class MessageFilterChain implements MessageFilter {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private MessageFilterChain() {

    }

    private List<MessageFilter> messageFilterList = new ArrayList<>();

    private ReentrantReadWriteLock reentrantReadWriteLock = new ReentrantReadWriteLock();

    private ReentrantReadWriteLock.ReadLock readLock = reentrantReadWriteLock.readLock();

    private ReentrantReadWriteLock.WriteLock writeLock = reentrantReadWriteLock.writeLock();

    private static volatile MessageFilterChain messageFilterChain = null;


    public static MessageFilterChain getInstance() {
        if (messageFilterChain == null) {
            synchronized (MessageFilterChain.class) {
                if (messageFilterChain == null) {
                    messageFilterChain = new MessageFilterChain();
                }
            }
        }
        return messageFilterChain;
    }

    @Override
    public boolean filter(CanalEntry.Entry entry) {
        try {
            readLock.lock();
            boolean accept = true;
            for (MessageFilter filter : messageFilterList) {
                accept &= filter.filter(entry);
            }
            return accept;
        } finally {
            readLock.unlock();
        }

    }


    public void register(MessageFilter messageFilter) {
        try {
            writeLock.lock();
            messageFilterList.add(messageFilter);
            logger.info(messageFilter.getClass().getSimpleName() + " has benn registered to message filter chain ");
        } finally {
            writeLock.unlock();
        }
    }


}
