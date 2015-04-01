package org.apache.nextsql.multipaxos;

public class PaxosConfigKeys {
  public static final String  PAXOS_WORKERTHREAD_MIN = "paxos.workerthread.min";
  public static final int     PAXOS_WORKERTHREAD_MIN_DEFAULT = 16;
  public static final String  PAXOS_WORKERTHREAD_MAX = "paxos.workerthread.max";
  public static final int     PAXOS_WORKERTHREAD_MAX_DEFAULT = 64;
}
