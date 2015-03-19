package org.apache.nextsql.multipaxos.nodemanager;

import java.util.List;

import org.apache.nextsql.common.NextSqlException;
import org.apache.nextsql.thrift.TNetworkAddress;

public interface INodeManager {
  public TNetworkAddress getNodeInfo(Long aNodeId);
  public Long getNodeId(TNetworkAddress aAddr);
  public long addNode(TNetworkAddress aAddr) throws NextSqlException;
  public void removeNode(Long aNodeId);
  public boolean isNodeIdExists(long aNId);
  public List<Long> getRandomNodeList(int aSize) throws NextSqlException;
  public List<Long> getAllNodeIds();
  public long getMyNodeId();
}
