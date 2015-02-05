package org.apache.nextsql.server;

public abstract class NextSqlException extends java.lang.Exception {
  public NextSqlException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  protected NextSqlException(String msg) {
    super(msg);
  }
}
