package org.apache.nextsql.client;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.nextsql.thrift.ReplicaService;
import org.apache.nextsql.thrift.TExecuteOperationReq;
import org.apache.nextsql.thrift.TExecuteOperationResp;
import org.apache.nextsql.thrift.TGetCBlockMetaReq;
import org.apache.nextsql.thrift.TGetCBlockMetaResp;
import org.apache.nextsql.thrift.TLeaderRep;
import org.apache.nextsql.thrift.TOpType;
import org.apache.nextsql.thrift.TOpenFileReq;
import org.apache.nextsql.thrift.TOpenFileResp;
import org.apache.nextsql.thrift.TOperation;
import org.apache.nextsql.thrift.TRWparam;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class NSOperation {
  private NSConnection _connection;
  // make connection per op if needed
  private TTransport _transport = null;
  private ReplicaService.Iface _client;
  private String _host;
  private int _port;
  
  private final long _opId;
  private boolean _isClosed = false;
  private long _defaultRWOffset = 0L;
  private long _defaultRWSize = 1024L;
  private static ReentrantLock _transportLock = new ReentrantLock(true);
  
  public class Block {
    final String _blkId;
    Block(String aBlkId) {
      this._blkId = aBlkId;
    }
  }
  
  public static enum OpType {
    READ,
    WRITE,
    UPDATE,
    GETMETA,
    SETMETA
  }
  
  public NSOperation(NSConnection aConn, ReplicaService.Iface aClient, long aOpId) {
    this._connection = aConn;
    this._client = aClient;
    this._opId = aOpId;
    this._host = _connection._host;
    this._port = _connection._port;
  }
  
  public Block Openfile(String aFilename, String aMode) throws NSQLException {
    long version = (NSConnection._blockMeta != null)? NSConnection._blockMeta.version: 0L;
    try {
      TOpenFileReq req = new TOpenFileReq(aFilename, version);
      TOpenFileResp resp = _client.OpenFile(req);
      Utils.verifySuccess(resp.getStatus());
      if (resp.isSetBlkmeta()) {
        NSConnection._blockMeta = resp.blkmeta;
      }
      return new Block(resp.blkId);
    } catch (NSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new NSQLException(e.toString(), e);
    }
  }
  
  public NSResultSet execute(Block aBlk, OpType aType, String aInBuff)
      throws NSQLException {
    return execute(aBlk, aType, aInBuff, _defaultRWOffset, _defaultRWSize, null);
  }
  
  public NSResultSet execute(Block aBlk, OpType aType, String aInBuff,
      long aOffset, long aSize) throws NSQLException {
    return execute(aBlk, aType, aInBuff, aOffset, aSize, null);
  }
  
  public NSResultSet execute(String aFilename, OpType aType, String aInBuff)
      throws NSQLException {
    return execute(null, aType, aInBuff, _defaultRWOffset, _defaultRWSize, aFilename);
  }
  
  public NSResultSet execute(String aFilename, OpType aType, String aInBuff,
      long aOffset, long aSize) throws NSQLException {
    return execute(null, aType, aInBuff, aOffset, aSize, aFilename);
  }
  
  public NSResultSet execute(Block aBlk, OpType aType, String aInBuff,
      long aOffset, long aSize, String aFilename) throws NSQLException {
    TOperation op;
    TOpType type;
    switch (aType) {
      case READ:    type = TOpType.OP_READ; break;
      case WRITE:   type = TOpType.OP_WRITE; break;
      case UPDATE:  type = TOpType.OP_UPDATE; break;
      case GETMETA: type = TOpType.OP_GETMETA; break;
      case SETMETA: type = TOpType.OP_SETMETA; break;
      default:
        throw new NSQLException("Unknown operation type.");
    }
    op = new TOperation(0L, _opId, type);
    op.setRw_param(new TRWparam(aInBuff, aOffset, aSize));
    
    if (aType == OpType.READ || aType == OpType.WRITE || aType == OpType.UPDATE) {
      // These ops should be requested to the leader. so check leader location
      if (_connection._blockMeta != null) {
        TLeaderRep laddr = null;
        if (aBlk != null) {
          laddr = _connection._blockMeta.blkidleader_map.get(aBlk._blkId);
        } else if (aFilename != null) {
          List<String> blkIds = _connection._blockMeta.fileblkid_map
              .get(aFilename);
          laddr = _connection._blockMeta.blkidleader_map.get(blkIds.get(0));
        }
        if (laddr != null) {
          if (!(laddr.leader_repaddr.hostname.equals(_host)) ||
              !(laddr.leader_repaddr.rsm_port == _port)) {
            reConnect(laddr.leader_repaddr.hostname, laddr.leader_repaddr.rsm_port);
          }
        }
      } else {
        try {
          // update block meta
          TGetCBlockMetaResp getMetaResp =
            _client.GetCBlockMeta(
              new TGetCBlockMetaReq(
                (_connection._blockMeta != null)? _connection._blockMeta.version: 0L
              )
            );
          Utils.verifySuccess(getMetaResp.getStatus());
          if (getMetaResp.isSetBlkmeta()) {
            _connection._blockMeta = getMetaResp.blkmeta;
          }
        } catch (NSQLException e) {
          throw e;
        } catch (Exception e) {
          throw new NSQLException(e.toString(), e);
        }
      }
    }
    
    try {
      TExecuteOperationReq req;
      if (aBlk == null && aFilename != null) {
        req = new TExecuteOperationReq(null, op, _connection._blockMeta.version);
        req.setFile_path(aFilename);
      } else {
        req = new TExecuteOperationReq(aBlk._blkId, op, _connection._blockMeta.version);
      }
      TExecuteOperationResp resp = _client.ExecuteOperation(req);
      Utils.verifySuccess(resp.getStatus());
      return new NSResultSet(resp.result);
    } catch (NSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new NSQLException(e.toString(), e);
    }
  }
  
  synchronized private void reConnect(String aHost, int aPort) throws NSQLException {
    openTransportInOp();
    _host = aHost;
    _port = aPort;
  }
  
  public void close() {
    if (_isClosed)
      return;
    // TODO: send close request to a server to free server-side resources
    // _client.closeop...
    _isClosed = true;
    _client = null;
    if (_transport != null) {
      _transport.close();
    }
  }
  
  public boolean isClosed() {
    return _isClosed;
  }
  
  protected void openTransportInOp() throws NSQLException {
    _transport = new TSocket(_host, _port, _connection._socketTimeout);

    TProtocol protocol = new TBinaryProtocol(_transport);
    _client = new ReplicaService.Client(protocol);
    
    try {
      _transport.open();
    } catch (TTransportException e) {
      throw new NSQLException("Could not establish connection to " + _host + ":"
        + _port + e.getMessage(), e);
    }
  }
}
