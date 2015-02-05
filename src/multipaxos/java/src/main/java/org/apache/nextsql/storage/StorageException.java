package org.apache.nextsql.storage;

public abstract class StorageException extends java.lang.Exception {
  public StorageException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  protected StorageException(String msg) {
    super(msg);
  }
}
