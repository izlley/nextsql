package org.apache.nextsql.client;

public class NSQLException extends java.lang.Exception{
  public NSQLException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  protected NSQLException(String msg) {
    super(msg);
  }
}
