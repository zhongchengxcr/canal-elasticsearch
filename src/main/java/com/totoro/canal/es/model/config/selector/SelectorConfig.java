package com.totoro.canal.es.model.config.selector;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/11/19 下午6:04
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class SelectorConfig {

    /**
     * 过滤器  database.table
     */
    private String filterPatten;

    private String canalAddress;

    private String canalUserName;

    private String canalPassword;

    private String destination;

    private String canalMode;

    public String getFilterPatten() {
        return filterPatten;
    }

    public void setFilterPatten(String filterPatten) {
        this.filterPatten = filterPatten;
    }

    public String getCanalAddress() {
        return canalAddress;
    }

    public void setCanalAddress(String canalAddress) {
        this.canalAddress = canalAddress;
    }

    public String getCanalUserName() {
        return canalUserName;
    }

    public void setCanalUserName(String canalUserName) {
        this.canalUserName = canalUserName;
    }

    public String getCanalPassword() {
        return canalPassword;
    }

    public void setCanalPassword(String canalPassword) {
        this.canalPassword = canalPassword;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getCanalMode() {
        return canalMode;
    }

    public void setCanalMode(String canalMode) {
        this.canalMode = canalMode;
    }
}
