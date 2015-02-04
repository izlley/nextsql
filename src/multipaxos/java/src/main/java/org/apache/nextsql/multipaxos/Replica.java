package org.apache.nextsql.multipaxos;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

import main.java.org.apache.nextsql.storage.IStorage;

public class Replica implements ReplicaService.Iface {
  private boolean _leader = false;
  private long _blockid;
  private TNetworkAddress _leaderLoc;
  private List<TNetworkAddress> _locations;
  private long _size = 0;
  
  // need more think...
  public static enum State {
    ACTIVE,
    RECOVERY,
    CLOSED
  }
  private State _state = State.ACTIVE;
  
  // must be atomic?
  private AtomicLong _slotNum = new AtomicLong(0);
  private long _decisionSlotNum = 1;
  private HashMap<Long, TOperation> _proposals = new HashMap<Long, TOperation>();
  private HashMap<Long, TOperation> _decisions = new HashMap<Long, TOperation>();
  private IStorage _storage = null;
  
  public Replica(long aBlkId, List<TNetworkAddress> aLocs, boolean aLeader,
      TNetworkAddress aLeaderAddr, IStorage aStorage) {
    this._blockid = aBlkId;
    this._leaderLoc = aLeaderAddr;
    this._locations = aLocs;
    this._leader = aLeader;
    this._storage = aStorage;
  }
  
  public TExecuteOperationResp ExecuteOperation(TExecuteOperationReq req) {
    long newSlotNum = _slotNum.incrementAndGet();
  }
}
