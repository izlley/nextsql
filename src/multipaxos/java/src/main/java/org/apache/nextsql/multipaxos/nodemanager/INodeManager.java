package org.apache.nextsql.multipaxos.nodemanager;

import org.apache.nextsql.common.NextSqlException;
import org.apache.nextsql.thrift.TNetworkAddress;

public interface INodeManager {
  public TNetworkAddress getNode(Long aNodeId);
  public Long getNodeId(TNetworkAddress aAddr);
  public long addNode(TNetworkAddress aAddr) throws NextSqlException;
  public void removeNode(Long aNodeId);
  public boolean isNodeIdExists(Long aNId);
}
