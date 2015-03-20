package org.apache.nextsql.common;

public class NextSqlException extends java.lang.Exception {
  public NextSqlException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  public NextSqlException(String msg) {
    super(msg);
  }
}
