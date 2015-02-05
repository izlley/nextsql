package org.apache.nextsql.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.nextsql.multipaxos.Leader;
import org.apache.nextsql.multipaxos.thrift.TNetworkAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemInfo {
  private static final Logger LOG = LoggerFactory.getLogger(SystemInfo.class);
  
  public static final int serverPort = 6652;
  
  public static TNetworkAddress getNetworkAddress() {
    TNetworkAddress addr = new TNetworkAddress();
    try {
      addr.hostname = InetAddress.getLocalHost().getHostAddress();
      addr.port = serverPort;
    } catch (UnknownHostException e) {
      LOG.error("getNetworkAddress failed: " + e.getMessage());
      return null;
    }
    return addr;
  }
}
