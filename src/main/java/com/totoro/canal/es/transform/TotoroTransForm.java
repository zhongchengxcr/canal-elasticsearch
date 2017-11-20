package com.totoro.canal.es.transform;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.totoro.canal.es.channel.TotoroChannel;
import com.totoro.canal.es.model.es.ElasticsearchMetadata;

import java.util.concurrent.Callable;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/11/19 下午6:44
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class TotoroTransForm implements TransForm<Message, ElasticsearchMetadata>, Callable<ElasticsearchMetadata> {

    private TotoroChannel channel;

    public TotoroTransForm(TotoroChannel channel) {
        this.channel = channel;
    }

    @Override
    public ElasticsearchMetadata call() throws Exception {
        Message message = channel.takeMessage();
        return trans(message);
    }


    @Override
    public ElasticsearchMetadata trans(Message input) {
        System.out.println(Thread.currentThread().getName() + "处理消息id ：" + input.getId());

        long sum = input.getEntries().stream().filter((s) -> {

            if (s.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) {
                return true;
            } else {
                return false;
            }
        }).count();
        return new ElasticsearchMetadata().setId(String.valueOf(sum));
    }
}
