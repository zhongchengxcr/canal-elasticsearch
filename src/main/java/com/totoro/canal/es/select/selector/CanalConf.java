package com.totoro.canal.es.select.selector;

import com.google.common.base.Preconditions;
import com.totoro.canal.es.common.TotoroException;
import com.totoro.canal.es.select.selector.canal.CanalEmbedSelector;
import com.totoro.canal.es.util.IPAddressUtil;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/12/04 上午10:50
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class CanalConf implements Serializable {

    private CanalEmbedSelector.Mode mode = CanalEmbedSelector.Mode.SIGN;

    //#canal的实例名字
    private String destination;

    private String filterPatten = "";

    //canal 地址
    private String address;

    private String zkAddress;

    private String userName;

    private String passWord;

    private String accept;


    public CanalEmbedSelector.Mode getMode() {
        return mode;
    }

    public CanalConf setMode(String mode) {
        if (!StringUtils.isEmpty(mode)) {
            mode = mode.toUpperCase();
            try {
                this.mode = CanalEmbedSelector.Mode.valueOf(mode);
            } catch (IllegalArgumentException e) {
                throw new TotoroException("no match patten,mode:" + mode);
            }
        }
        return this;
    }

    public String getDestination() {
        return destination;
    }

    public CanalConf setDestination(String destination) {
        this.destination = destination;
        return this;
    }

    public String getAddress() {
        return address;
    }

    public CanalConf setAddress(String address) {
        this.address = address;
        return this;
    }

    public String getFilterPatten() {
        return filterPatten;
    }

    public CanalConf setFilterPatten(String filterPatten) {
        if (!StringUtils.isEmpty(filterPatten)) {
            this.filterPatten = filterPatten;
        }

        return this;
    }

    public String getZkAddress() {
        return zkAddress;
    }

    public CanalConf setZkAddress(String zkAddress) {
        this.zkAddress = zkAddress;
        return this;
    }

    public String getUserName() {
        return userName;
    }

    public CanalConf setUserName(String userName) {
        this.userName = userName;
        return this;
    }

    public String getPassWord() {
        return passWord;
    }

    public CanalConf setPassWord(String passWord) {
        this.passWord = passWord;
        return this;
    }


    public String getAccept() {
        return accept;
    }

    public CanalConf setAccept(String accept) {
        this.accept = accept;
        return this;
    }

    public CanalConf builder() {

        Preconditions.checkArgument(StringUtils.isNotEmpty(destination), "Illegal destination , destination can't be empty");
        Preconditions.checkArgument(StringUtils.isNotEmpty(accept), "Illegal accept , accept can't be empty");

        if (CanalEmbedSelector.Mode.SIGN.equals(mode)) {
            Preconditions.checkArgument(IPAddressUtil.isAddress(address), "Illegal address : %s", address);

        } else if (CanalEmbedSelector.Mode.CLUSTER.equals(mode)) {
            Preconditions.checkArgument(IPAddressUtil.isAddress(address), "Illegal zkAddress : %s", zkAddress);
        }

        return this;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CanalConf{");
        sb.append("mode=").append(mode);
        sb.append(", destination='").append(destination).append('\'');
        sb.append(", filterPatten='").append(filterPatten).append('\'');
        sb.append(", address='").append(address).append('\'');
        sb.append(", zkAddress='").append(zkAddress).append('\'');
        sb.append(", userName='").append(userName).append('\'');
        sb.append(", accept='").append(accept).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
