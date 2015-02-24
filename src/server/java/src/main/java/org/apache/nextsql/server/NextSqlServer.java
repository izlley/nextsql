package org.apache.nextsql.server;

import org.apache.nextsql.multipaxos.Replica;
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
      // run a thrift server for managing replicas
      
      
      
      // run multi-paxos thrift server
      
      //CLIHandler handler = new CLIHandler();
      //final TCLIService.Processor processor = new TCLIService.Processor(handler);
      
      Runnable sThriftServer = new Runnable() {
        public void run() {
          //createServer(processor);
        }
      };
      /*
      Runnable secure = new Runnable() {
        public void run() {
          secure(processor);
        }
      };
      */
      
      // Add shutdown hook.
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          String shutdownMsg = "Shutting down querycache.";
          LOG.info(shutdownMsg);
        }
      });

      new Thread(sThriftServer).start();
      //new Thread(secure).start();
    } catch (Exception e) {
      LOG.error("FATAL error : ", e);
      System.exit(1);
    }
  }
}
