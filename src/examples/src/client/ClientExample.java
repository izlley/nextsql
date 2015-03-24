package client;

import org.apache.nextsql.client.NSConnection;
import org.apache.nextsql.client.NSOperation;
import org.apache.nextsql.client.NSOperation.NSBlock;
import org.apache.nextsql.client.NSOperation.NSOpType;
import org.apache.nextsql.client.NSQLException;
import org.apache.nextsql.client.NSResultSet;

public class ClientExample {
  private static int _loopcnt = 1;
  private static String _hostname = null;
  private static int _port = 6652;
  
  public static void main(String[] args) {
    // parsing flags
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-loopcnt")) {
        _loopcnt = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-host")) {
        _hostname = args[++i];
      } else if (args[i].equals("-port")) {
        _port = Integer.parseInt(args[++i]);
      } else {
        exit("Unknowun argument: " + args[i]);
      }
    }
    if (_hostname == null) {
      exit("hostname is not set correctly.");
    }
    
    NSConnection conn = null;
    NSOperation op = null;
    for (int j = 0; j < _loopcnt; j++) {
      try {
        // connect to nextsql
        conn = new NSConnection(_hostname, _port, 0);
        conn.getConnectoin();
        // make operation
        op = conn.createOperation();
        // open file
        NSBlock blk = op.Openfile("file.tmp", null);
        // exec operation
        NSResultSet result = op.execute(blk, NSOpType.READ, null);
        // print result
        System.out.println("result code = " + result.getState()
            + ", result buff = " + result.getBuffer());
        // close op
        op.close();
        // close conn
        conn.close();
      } catch (NSQLException e) {
        System.out.println("ERROR] " + e.getMessage());
      } finally {
        if (op != null) {
          op.close();
        } 
        if (conn != null) {
          try {
            conn.close();
          } catch (NSQLException e) {
            System.out.println("ERROR] " + e.getMessage());
          }
        }
      }
    }
  }
  
  private static void exit(String errmsg) {
    System.out.println("ERROR]" + errmsg);
    help();
    System.exit(1);
  }
  
  private static void help() {
    System.out.println("Options> -loopcnt, -host, -port");
  }
}
