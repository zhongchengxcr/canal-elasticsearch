package com.totoro.canal.es.transform;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.totoro.canal.es.model.es.ElasticsearchMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private Logger logger = LoggerFactory.getLogger(getClass());

    private Message message;

    private TransForm transForm;

    public TotoroTransForm(Message message) {
        this.message = message;
    }

    @Override
    public ElasticsearchMetadata call() throws Exception {
        return trans(message);
    }

    @Override
    public ElasticsearchMetadata trans(Message input) {
        logger.info(Thread.currentThread().getName() + "处理消息id ：" + input.getId());
        long sum = input.getEntries().stream().filter((s) -> s.getEntryType().equals(CanalEntry.EntryType.ROWDATA)).count();
        return new ElasticsearchMetadata().setId(String.valueOf(sum)).setBatchId(input.getId());
    }

    public TransForm getTransForm() {
        return transForm;
    }

    public TotoroTransForm setTransForm(TransForm transForm) {
        this.transForm = transForm;
        return this;
    }
}
