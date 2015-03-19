package org.apache.nextsql.client;

import org.apache.nextsql.thrift.TExecResult;

public class NSResultSet {
  private String _buffer;
  private long _retvalue;
  public NSResultSet() {
    this._buffer = null;
    this._retvalue = -1L;
  }
  
  public NSResultSet(TExecResult aExecResult) {
    if (aExecResult != null) {
      this._buffer = aExecResult.buffer;
      this._retvalue = aExecResult.retval;
    } else {
      this._buffer = null;
      this._retvalue = -1L;
    }
  }
  
  protected void setResult(TExecResult aExecResult) {
    _buffer = aExecResult.buffer;
    _retvalue = aExecResult.retval;
  }
  public String getBuffer() {
    return _buffer;
  }
  public long getState() {
    return _retvalue;
  }
}
