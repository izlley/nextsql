package org.apache.nextsql.server;

import org.apache.nextsql.multipaxos.thrift.PaxosService;
import org.apache.nextsql.multipaxos.thrift.ReplicaService;
import org.apache.nextsql.util.TServerSocketKeepAlive;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NextSqlServer {
  private static final Logger LOG = LoggerFactory.getLogger(NextSqlServer.class);
  public static final NextSqlConfiguration _conf = new NextSqlConfiguration();
  
  private static final BlockManager _blkMgr = new BlockManager(_conf.getInt(
      NextSqlConfigKeys.NS_FILE_REPLICATION, NextSqlConfigKeys.NS_FILE_REPLICATION_DEFAULT));
  // private static final StorageManager _strgMgr;
  
  public static void main(String [] args) {
    try {
      // replica thrift server
      final ReplicaService.Processor repSvrProc = new ReplicaService.Processor(
        new ReplicaTServer(_blkMgr));
      
      // multi-paxos thrift server
      final PaxosService.Processor paxosSvrProc = new PaxosService.Processor(
        new PaxosTServer(_blkMgr));
      
      Runnable replicaTServer = new Runnable() {
        public void run() {
          createReplicaServer(repSvrProc);
        }
      };
      
      Runnable paxosTServer = new Runnable() {
        public void run() {
          createPaxosServer(paxosSvrProc);
        }
      };
      
      // Add shutdown hook.
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          final String shutdownMsg = "Shutting down NextSql.";
          LOG.info(shutdownMsg);
        }
      });

      // run the replica thrift server
      Thread repSvrThread = new Thread(replicaTServer);
      repSvrThread.start();
      // run the multi-paxos thrift server
      Thread paxosSvrThread = new Thread(paxosTServer);
      paxosSvrThread.start();
      LOG.info("NextSql is started.");
    } catch (Exception e) {
      LOG.error("FATAL error : ", e);
      System.exit(1);
    }
  }
  
  public static void createReplicaServer(ReplicaService.Processor processor) {
    try {
      boolean tcpKeepAlive = true;
      
      TServerTransport serverTransport = tcpKeepAlive ?
        new TServerSocketKeepAlive(
          _conf.getInt(NextSqlConfigKeys.NS_REPLICA_SERVER_PORT,
                       NextSqlConfigKeys.NS_REPLICA_SERVER_PORT_DEFAULT)
        ): new TServerSocket(
          _conf.getInt(NextSqlConfigKeys.NS_REPLICA_SERVER_PORT,
                       NextSqlConfigKeys.NS_REPLICA_SERVER_PORT_DEFAULT)
        );
      TThreadPoolServer.Args sArgs = new TThreadPoolServer.Args(serverTransport).
          processor(processor);
      sArgs.inputProtocolFactory(new TCompactProtocol.Factory());
      sArgs.outputProtocolFactory(new TCompactProtocol.Factory());
      sArgs.minWorkerThreads(_conf.getInt(NextSqlConfigKeys.NS_REPLICA_WORKERTHREAD_MIN,
        NextSqlConfigKeys.NS_REPLICA_WORKERTHREAD_MIN_DEFAULT));
      sArgs.maxWorkerThreads(_conf.getInt(NextSqlConfigKeys.NS_REPLICA_WORKERTHREAD_MAX,
        NextSqlConfigKeys.NS_REPLICA_WORKERTHREAD_MAX_DEFAULT));
      TServer server = new TThreadPoolServer(sArgs);
      
      System.out.println("Starting the NextSql-replica server...");
      LOG.info("Starting the NextSql-replica server...");
      server.serve();
    } catch (Exception e) {
      LOG.error("FATAL error : ", e);
      System.exit(1);
    }
  }
  
  public static void createPaxosServer(PaxosService.Processor processor) {
    try {
      boolean tcpKeepAlive = true;
      
      TServerTransport serverTransport = tcpKeepAlive ?
        new TServerSocketKeepAlive(
          _conf.getInt(NextSqlConfigKeys.NS_PAXOS_SERVER_PORT,
                       NextSqlConfigKeys.NS_PAXOS_SERVER_PORT_DEFAULT)
        ): new TServerSocket(
          _conf.getInt(NextSqlConfigKeys.NS_PAXOS_SERVER_PORT,
                       NextSqlConfigKeys.NS_PAXOS_SERVER_PORT_DEFAULT)
        );
      TThreadPoolServer.Args sArgs = new TThreadPoolServer.Args(serverTransport).
          processor(processor);
      sArgs.inputProtocolFactory(new TCompactProtocol.Factory());
      sArgs.outputProtocolFactory(new TCompactProtocol.Factory());
      sArgs.minWorkerThreads(_conf.getInt(NextSqlConfigKeys.NS_PAXOS_WORKERTHREAD_MIN,
        NextSqlConfigKeys.NS_PAXOS_WORKERTHREAD_MIN_DEFAULT));
      sArgs.maxWorkerThreads(_conf.getInt(NextSqlConfigKeys.NS_PAXOS_WORKERTHREAD_MAX,
        NextSqlConfigKeys.NS_PAXOS_WORKERTHREAD_MAX_DEFAULT));
      TServer server = new TThreadPoolServer(sArgs);
      
      System.out.println("Starting the NextSql-replica server...");
      LOG.info("Starting the NextSql-replica server...");
      server.serve();
    } catch (Exception e) {
      LOG.error("FATAL error : ", e);
      System.exit(1);
    }
  }
}
