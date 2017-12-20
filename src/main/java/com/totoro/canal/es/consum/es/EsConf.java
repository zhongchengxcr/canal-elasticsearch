package com.totoro.canal.es.consum.es;

import com.google.common.base.Preconditions;
import com.totoro.canal.es.util.IPAddressUtil;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/12/19 下午4:39
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class EsConf implements Serializable {


    private String address;

    private String username;

    private String password;

    private String clusterName;


    public String getAddress() {
        return address;
    }

    public EsConf setAddress(String address) {
        this.address = address;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public EsConf setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public EsConf setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getClusterName() {
        return clusterName;
    }

    public EsConf setClusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }


    public EsConf builder() {
        Preconditions.checkArgument(!StringUtils.isEmpty(address));
        Preconditions.checkArgument(!StringUtils.isEmpty(clusterName));
        Preconditions.checkArgument(IPAddressUtil.isAddress(address));
        return this;
    }
}
