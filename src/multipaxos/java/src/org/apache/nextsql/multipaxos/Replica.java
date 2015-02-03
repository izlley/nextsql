package org.apache.nextsql.multipaxos;

import java.util.HashMap;
import org.apache.nextsql.storage.IStorage;

public class Replica implements ReplicaService {
  private boolean _leader = false;
  private long _blockid;
  private TNetworkAddress _location;
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
  private long _slotNum = 1;
  private HashMap<Long, TOperation> _proposals = new HashMap<Long, TOperation>();
  private HashMap<Long, TOperation> _decisions = new HashMap<Long, TOperation>();
  private IStorage _storage = null;
  
  public Replica(long aBlkId, List<TNetworkAddress> aLocs, boolean aLeader,
      IStorage aStorage) {
    this._blockid = aBlkId;
    this._location = new TNetworkAddress("",0);
    this._locations = aLocs;
    this._leader = aLeader;
    this._storage = aStorage;
  }
  
}
