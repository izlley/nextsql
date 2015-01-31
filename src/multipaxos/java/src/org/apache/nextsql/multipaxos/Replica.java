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
  private static HashMap<Long, Operation> _proposals = new HashMap<Long, Operation>();
  private static HashMap<Long, Operation> _decisions = new HashMap<Long, Operation>();
  private IStorage _storage = null;
  
  public Replica(IStorage aStorage) {
    this._storage = aStorage;
  }
  
}
