package org.apache.nextsql.server;

import org.apache.nextsql.common.NextSqlException;

public class NextSqlServerException extends NextSqlException {
  public NextSqlServerException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  protected NextSqlServerException(String msg) {
    super(msg);
  }
}