package org.apache.nextsql.multipaxos;

import java.util.HashMap;
import org.apache.nextsql.storage.IStorage;

public class Replica {
  // need more think...
  public static enum State {
    ACTIVE,
    RECOVERY,
    CLOSED
  }
  
  // must be atomic?
  private static long _slot_num = 1;
  private static HashMap<Long, TOperation> _proposals = new HashMap<Long, TOperation>();
  private static HashMap<Long, TOperation> _decisions = new HashMap<Long, TOperation>();
  private IStorage _storage = null;
  
  public Replica(IStorage aStorage) {
    this._storage = aStorage;
  }
  
}
