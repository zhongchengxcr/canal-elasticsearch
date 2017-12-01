package com.totoro.canal.es.common;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/12/01 下午12:53
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class TotoroException extends RuntimeException {

    private String errMsg;

    public TotoroException(String errMsg) {
        super(errMsg);
        this.errMsg = errMsg;
    }

    public String getErrMsg() {
        return errMsg;
    }
}
