package org.apache.nextsql.multipaxos.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.nextsql.multipaxos.Leader;
import org.apache.nextsql.multipaxos.thrift.TNetworkAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemInfo {
  private static final Logger LOG = LoggerFactory.getLogger(SystemInfo.class);
  
  public static final int _serverPort = 6652;
  private static TNetworkAddress _netAddress = null;
  
  public static TNetworkAddress getNetworkAddress() {
    if (_netAddress != null) return _netAddress;
    TNetworkAddress addr = new TNetworkAddress();
    try {
      addr.hostname = InetAddress.getLocalHost().getHostAddress();
      addr.port = _serverPort;
    } catch (UnknownHostException e) {
      LOG.error("getNetworkAddress failed: " + e.getMessage());
      return null;
    }
    _netAddress = addr;
    return addr;
  }
}
