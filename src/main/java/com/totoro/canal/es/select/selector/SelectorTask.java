package com.totoro.canal.es.select.selector;

import com.alibaba.otter.canal.protocol.Message;
import com.totoro.canal.es.channel.TotoroChannel;
import com.totoro.canal.es.common.task.GlobalTask;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/12/01 下午4:42
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class SelectorTask extends GlobalTask {

    private TotoroSelector totoroSelector;

    private TotoroChannel channel;

    public SelectorTask(TotoroSelector totoroSelector, TotoroChannel channel) {
        this.totoroSelector = totoroSelector;
        this.channel = channel;
    }

    @Override
    public void run() {
        running = true;
        totoroSelector.start();
        totoroSelector.rollback();
        while (running) {
            Message message;
            try {
                message = totoroSelector.selector();
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    System.out.println("空数据");
                } else {
                    System.out.println("放入数据");
                    channel.putMessage(message);
                    totoroSelector.ack(batchId); // 提交确认
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
