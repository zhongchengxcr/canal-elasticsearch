package test.syn;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;

import java.net.InetSocketAddress;
import java.util.concurrent.locks.LockSupport;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/12/04 下午2:13
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class LockTest {

    static boolean running = true;

    private static final int maxEmptyTimes = 10;

    public static void main(String[] args) throws InterruptedException {

        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("127.0.0.1", 11111),
                "totoro",
                "",
                "");

        connector.connect();
        connector.subscribe();
        int emptyTimes = 0;

        while (running) {
            Message message = connector.getWithoutAck(5 * 1024);

            if (message == null || message.getId() == -1L) {
                applyWait(emptyTimes++);
            } else {
                //logger.info(message.toString());
                long messageId = message.getId();
                System.out.println("消息id：" + messageId);
                Thread.sleep(1000);

                connector.rollback();
            }


        }

    }

    private static void applyWait(int emptyTimes) throws InterruptedException {
        int newEmptyTimes = emptyTimes > maxEmptyTimes ? maxEmptyTimes : emptyTimes;
        if (emptyTimes >= 3) {
            Thread.yield();
            LockSupport.parkNanos(10000 * 1000L * newEmptyTimes);
        }
    }

}
