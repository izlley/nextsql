package org.apache.nextsql.multipaxos.storage;

import org.apache.nextsql.common.NextSqlException;

public class StorageException extends NextSqlException {
  public StorageException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  protected StorageException(String msg) {
    super(msg);
  }
}
