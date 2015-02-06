package org.apache.nextsql.storage;

import org.apache.nextsql.server.NextSqlException;

public class StorageException extends NextSqlException {
  public StorageException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  protected StorageException(String msg) {
    super(msg);
  }
}