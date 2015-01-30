package org.apache.nextsql.multipaxos;

public class Replica {
  public static enum State {
    ACTIVE, RECOVERY, CLOSED
  }
  
  // is atomic?
  static long _slot_num = 1;
  HashMap<long, >
}
