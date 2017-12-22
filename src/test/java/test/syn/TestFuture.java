package test.syn;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/12/22 下午2:45
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class TestFuture extends FutureTask<String> {


    public TestFuture(Callable<String> callable) {
        super(callable);
    }


}
