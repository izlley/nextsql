package org.apache.nextsql.client;

import org.apache.nextsql.thrift.ReplicaService;
import org.apache.nextsql.thrift.TExecuteOperationReq;
import org.apache.nextsql.thrift.TExecuteOperationResp;
import org.apache.nextsql.thrift.TOpType;
import org.apache.nextsql.thrift.TOperation;

public class NSOperation {
  private NSConnection _connection;
  private ReplicaService.Iface _client;
  private final long _opId;
  private boolean _isClosed = false;
  
  public static enum OpType {
    READ,
    WRITE,
    OPEN,
    DELETE,
    UPDATE,
    GETMETA,
    SETMETA
  }
  
  public NSOperation(NSConnection aConn, ReplicaService.Iface aClient, long aOpId) {
    this._connection = aConn;
    this._client = aClient;
    this._opId = aOpId;
  }
  
  public boolean execute(String aFilename, OpType aType, String aBuffer)
      throws NSQLException {
    return execute(aFilename, aType, aBuffer, 0L, Long.MAX_VALUE);
  }
  
  public boolean execute(String aFilename, OpType aType, String aBuffer, long aOffset, long aSize)
      throws NSQLException {
    boolean result = false;
    TOperation op;
    TOpType type;
    switch (aType) {
      case READ:    type = TOpType.OP_READ; break;
      case WRITE:   type = TOpType.OP_WRITE; break;
      case OPEN:    type = TOpType.OP_OPEN; break;
      case DELETE:  type = TOpType.OP_DELETE; break;
      case UPDATE:  type = TOpType.OP_UPDATE; break;
      case GETMETA: type = TOpType.OP_GETMETA; break;
      case SETMETA: type = TOpType.OP_SETMETA; break;
      default:
        throw new NSQLException("Unknown operation type.");
    }
    op = new TOperation(0L, _opId, type, aBuffer);
    op.setOffset(aOffset);
    op.setSize(aSize);
    try {
      TExecuteOperationReq req = new TExecuteOperationReq(aFilename, op);
      TExecuteOperationResp resp = _client.ExecuteOperation(req);
      Utils.verifySuccess(resp.getStatus());
      if (resp.isSetData())
        result = true;
    } catch (NSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new NSQLException(e.toString(), e);
    }
    return result;
  }
  
  public void close() {
    if (_isClosed)
      return;
    // TODO: send close request to a server to free server-side resources
    // _client.closeop...
    _isClosed = true;
    _client = null;
  }
  
  public boolean isClosed() {
    return _isClosed;
  }
}
