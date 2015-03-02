package org.apache.nextsql.multipaxos.nodemanager;

import java.util.List;

import org.apache.nextsql.common.NextSqlException;
import org.apache.nextsql.multipaxos.thrift.TNetworkAddress;

public interface INodeManager {
  public TNetworkAddress getNode(Long aNodeId);
  public Long getNodeId(TNetworkAddress aAddr);
  public long addNode(TNetworkAddress aAddr) throws NextSqlException;
  public void removeNode(Long aNodeId);
  public boolean isNodeIdExists(Long aNId);
}
