package com.totoro.canal.es.util;

import org.apache.commons.lang.StringUtils;

/**
 * 说明 . <br>
 * <p>
 * <p>
 * Copyright: Copyright (c) 2017/12/04 下午12:46
 * <p>
 * Company: xxx
 * <p>
 *
 * @author zhongcheng_m@yeah.net
 * @version 1.0.0
 */
public class IPAddressUtil extends sun.net.util.IPAddressUtil {
   public static boolean isAddress(String address) {
        if (!StringUtils.isEmpty(address)) {
            String[] hostPort = address.trim().split(":");
            if (hostPort.length == 2) {
                String ip = hostPort[0];
                String portStr = hostPort[1];
                if (sun.net.util.IPAddressUtil.isIPv4LiteralAddress(ip) && StringUtils.isNumeric(portStr)) {
                    Integer port = Integer.valueOf(portStr);
                    return 0 < port && port < 65535;
                }
            }
        }
        return false;
    }
}
