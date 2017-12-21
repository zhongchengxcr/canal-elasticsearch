package com.totoro.canal.es.select.selector;

import com.alibaba.otter.canal.common.utils.BooleanMutex;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.totoro.canal.es.TotoroBootStrap;
import com.totoro.canal.es.channel.TotoroChannel;
import com.totoro.canal.es.common.RollBackMonitorFactory;
import com.totoro.canal.es.common.GlobalTask;
import org.apache.commons.lang.SystemUtils;
import org.springframework.util.CollectionUtils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

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

    private BooleanMutex rollBack = RollBackMonitorFactory.getBooleanMutex();

    private static String context_format = null;

    private static String row_format = null;

    private static String transaction_format = null;

    private static final String SEP = SystemUtils.LINE_SEPARATOR;

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);

    static {
        context_format = SEP + "****************************************************" + SEP;
        context_format += "* Batch Id: [{}] ,count : [{}] , memsize : [{}] , Time : {}" + SEP;
        context_format += "* Start : [{}] " + SEP;
        context_format += "* End : [{}] " + SEP;
        context_format += "****************************************************" + SEP;

        row_format = SEP
                + "----------------> binlog[{}:{}] , name[{},{}] , eventType : {} , executeTime : {} , delay : {}ms"
                + SEP;

        transaction_format = SEP + "================> binlog[{}:{}] , executeTime : {} , delay : {}ms" + SEP;

    }

    /**
     * 是否耦合度过高？？
     */

    public SelectorTask(TotoroSelector totoroSelector, TotoroChannel channel, TotoroBootStrap canalScheduler) {
        logger.info("Selector task init .......");
        this.totoroSelector = totoroSelector;
        this.channel = channel;
        logger.info("Selector task complete .......");
    }


    @Override
    public void run() {
        running = true;

        totoroSelector.start();
        totoroSelector.rollback();

        logger.info("Selector task start .......");
        Message message;
        rollBack.set(true);
        while (running) {
            try {
                //出现回滚立即停止
                message = totoroSelector.selector();


                /**
                 * 当前处理回滚的调度模型，可以保证在消费端出错的时候、正确处理回滚，并正确应答和继续消费数据。
                 *
                 * 当发生回滚时，首先 consumer task 会将 rollback 设置为true ,自己停止工作，等待唤醒
                 * 然后 trans task 也会同样挂起
                 * channel会拒绝接受 message 和 future ,对于已经提交的 future 会尝试取消
                 *
                 * 到此 除了 selector task 以外  的所有线程 全部尽最大努力去停止处理消息，但注意此时还没有回滚
                 *
                 * 因为 selector 是循环获取数据，每次循环都会判断 rollback 状态，一旦发现rollback状态，跳出循环
                 * 返回到 selector task里面 ，task会感知到回滚状态 ，清空渠道中的消息 ，并回滚 至最后一个未应答的
                 * 消费点，然后丢弃本条消息，重新获取一次消息（回滚的消息）
                 * 当上面所有工作 都做完了，便完成了回滚 ，selector task 改变回滚状态，重新正常工作
                 *
                 * 粗略测试结果 ： 40000条数据，单机测试，当 batchId 能被2整除的时候回滚
                 *  在不真正消费数据的前提下（消费端直接应答），处理性能非常好
                 *
                 */
                if (rollBack.state() == false) {
                    totoroSelector.rollback();
                    logger.info("The rollback happened =============>  discard message , batchId ：{}", message.getId());
                    //丢弃刚才的消息
                    message = totoroSelector.selector();
                    channel.clearMessage();
                    rollBack.set(true);
                }

                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    message = null;//help gc
                } else {
                    logger.info("Put message into channel =====> batchId :{}", message.getId());

                    if (logger.isDebugEnabled()) {
                        printSummary(message, batchId, size);
                        printEntry(message.getEntries());
                    }
                    //将消息放入管道
                    channel.putMessage(message);
                }
            } catch (InterruptedException e) {
                logger.error("Selector task has been interrupted ", e);
                running = false;
                break;
            }
        }
    }


    private void printSummary(Message message, long batchId, int size) {
        long memsize = 0;
        for (CanalEntry.Entry entry : message.getEntries()) {
            memsize += entry.getHeader().getEventLength();
        }

        String startPosition = null;
        String endPosition = null;
        if (!CollectionUtils.isEmpty(message.getEntries())) {
            startPosition = buildPositionForDump(message.getEntries().get(0));
            endPosition = buildPositionForDump(message.getEntries().get(message.getEntries().size() - 1));
        }

        logger.info(context_format, new Object[]{batchId, size, memsize, format.format(new Date()), startPosition,
                endPosition});
    }

    protected String buildPositionForDump(CanalEntry.Entry entry) {
        long time = entry.getHeader().getExecuteTime();
        Date date = new Date(time);
        return entry.getHeader().getLogfileName() + ":" + entry.getHeader().getLogfileOffset() + ":"
                + entry.getHeader().getExecuteTime() + "(" + format.format(date) + ")";
    }

    protected void printEntry(List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {
            long executeTime = entry.getHeader().getExecuteTime();
            long delayTime = System.currentTimeMillis() - executeTime;

            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN) {
                    CanalEntry.TransactionBegin begin = null;
                    try {
                        begin = CanalEntry.TransactionBegin.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务头信息，执行的线程id，事务耗时
                    logger.info(transaction_format,
                            new Object[]{entry.getHeader().getLogfileName(),
                                    String.valueOf(entry.getHeader().getLogfileOffset()),
                                    String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime)});
                    logger.info(" BEGIN ----> Thread id: {}", begin.getThreadId());
                } else if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                    CanalEntry.TransactionEnd end = null;
                    try {
                        end = CanalEntry.TransactionEnd.parseFrom(entry.getStoreValue());
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                    }
                    // 打印事务提交信息，事务id
                    logger.info("----------------\n");
                    logger.info(" END ----> transaction id: {}", end.getTransactionId());
                    logger.info(transaction_format,
                            new Object[]{entry.getHeader().getLogfileName(),
                                    String.valueOf(entry.getHeader().getLogfileOffset()),
                                    String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime)});
                }

                continue;
            }

            if (entry.getEntryType() == CanalEntry.EntryType.ROWDATA) {
                CanalEntry.RowChange rowChage = null;
                try {
                    rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }

                CanalEntry.EventType eventType = rowChage.getEventType();

                logger.info(row_format,
                        new Object[]{entry.getHeader().getLogfileName(),
                                String.valueOf(entry.getHeader().getLogfileOffset()), entry.getHeader().getSchemaName(),
                                entry.getHeader().getTableName(), eventType,
                                String.valueOf(entry.getHeader().getExecuteTime()), String.valueOf(delayTime)});

                if (eventType == CanalEntry.EventType.QUERY || rowChage.getIsDdl()) {
                    logger.info(" sql ----> " + rowChage.getSql() + SEP);
                    continue;
                }

                for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                    if (eventType == CanalEntry.EventType.DELETE) {
                        printColumn(rowData.getBeforeColumnsList());
                    } else if (eventType == CanalEntry.EventType.INSERT) {
                        printColumn(rowData.getAfterColumnsList());
                    } else {
                        printColumn(rowData.getAfterColumnsList());
                    }
                }
            }
        }
    }

    protected void printColumn(List<CanalEntry.Column> columns) {
        for (CanalEntry.Column column : columns) {
            StringBuilder builder = new StringBuilder();
            builder.append(column.getName() + " : " + column.getValue());
            builder.append("    type=" + column.getMysqlType());
            if (column.getUpdated()) {
                builder.append("    update=" + column.getUpdated());
            }
            builder.append(SEP);
            logger.info(builder.toString());
        }
    }

}
