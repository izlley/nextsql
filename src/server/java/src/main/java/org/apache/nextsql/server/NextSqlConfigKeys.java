package org.apache.nextsql.server;

public class NextSqlConfigKeys {
  public static final String  NS_REPLICA_SERVER_PORT = "ns.replica.server.port";
  public static final int     NS_REPLICA_SERVER_PORT_DEFAULT = 6652;
  public static final String  NS_REPLICA_WORKERTHREAD_MIN = "ns.replica.workerthread.min";
  public static final int     NS_REPLICA_WORKERTHREAD_MIN_DEFAULT = 16;
  public static final String  NS_REPLICA_WORKERTHREAD_MAX = "ns.replica.workerthread.max";
  public static final int     NS_REPLICA_WORKERTHREAD_MAX_DEFAULT = 64;
  public static final String  NS_PAXOS_SERVER_PORT = "ns.paxos.server.port";
  public static final int     NS_PAXOS_SERVER_PORT_DEFAULT = 6653;
  public static final String  NS_PAXOS_WORKERTHREAD_MIN = "ns.paxos.workerthread.min";
  public static final int     NS_PAXOS_WORKERTHREAD_MIN_DEFAULT = 16;
  public static final String  NS_PAXOS_WORKERTHREAD_MAX = "ns.paxos.workerthread.max";
  public static final int     NS_PAXOS_WORKERTHREAD_MAX_DEFAULT = 64;
  public static final String  NS_FILE_REPLICATION = "ns.replication";
  public static final short   NS_FILE_REPLICATION_DEFAULT = 3;
  public static final String  NS_CUSTOM_STORAGE = "ns.custom.storage";
  public static final String  NS_CUSTOM_STORAGE_DEFAULT = null;
  public static final String  NS_NODE_ADDR_LIST = "ns.node.addr.list";
  public static final String  NS_NODE_ADDR_LIST_DEFAULT = "localhost";
}
