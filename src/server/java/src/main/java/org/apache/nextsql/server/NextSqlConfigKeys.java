package org.apache.nextsql.server;

public class NextSqlConfigKeys {
  public static final String  NS_SERVER_PORT = "ns.server.port";
  public static final int     NS_SERVER_PORT_DEFAULT = 6652;
  public static final String  NS_WORKERTHREAD_MIN = "ns.workerthread.min";
  public static final int     NS_WORKERTHREAD_MIN_DEFAULT = 16;
  public static final String  NS_WORKERTHREAD_MAX = "ns.workerthread.max";
  public static final int     NS_WORKERTHREAD_MAX_DEFAULT = 64;
  public static final String  NS_SLAVES = "ns.slaves";
  public static final String  NS_SLAVES_DEFAULT = "localhost";
  public static final String  NS_FILE_REPLICATION = "ns.replication";
  public static final short   NS_FILE_REPLICATION_DEFAULT = 3;
}
