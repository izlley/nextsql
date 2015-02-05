package org.apache.nextsql.multipaxos;
import org.apache.nextsql.server.NextSqlException;

public class MultiPaxosException extends NextSqlException {
  public MultiPaxosException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  protected MultiPaxosException(String msg) {
    super(msg);
  }
}
