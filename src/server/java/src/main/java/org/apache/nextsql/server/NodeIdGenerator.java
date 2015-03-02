package org.apache.nextsql.server;

import org.apache.nextsql.util.SequentialNumber;

public class NodeIdGenerator extends SequentialNumber {
  private final NodeManager _nodeMgr;
  public NodeIdGenerator(NodeManager aNodeMgr) {
    super(0);
    this._nodeMgr = aNodeMgr;
  }
  
  @Override
  public long nextValue() {
    long nodeId = super.nextValue();
    while(!isValidNodeId(nodeId)) {
      nodeId = super.nextValue();
    }
    return nodeId;
  }
  
  private boolean isValidNodeId(long aNodeId) {
    return (_nodeMgr.isNodeIdExists(aNodeId) == false);
  }
}
