package org.apache.nextsql.client;

import java.util.List;
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
import org.apache.nextsql.thrift.TStatusCode;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class NSOperation {
  private NSConnection _connection;
  // make connection per op if needed
  private TTransport _transport = null;
  private ReplicaService.Iface _client;
  private String _host;
  private int _port;
  
  private boolean _isClosed = false;
  private long _defaultRWOffset = 0L;
  private long _defaultRWSize = 1024L;
  
  public class NSBlock {
    final String _blkId;
    NSBlock(String aBlkId) {
      this._blkId = aBlkId;
    }
  }
  
  public static enum NSOpType {
    READ,
    WRITE,
    UPDATE,
    GETMETA,
    SETMETA
  }
  
  public NSOperation(NSConnection aConn, ReplicaService.Iface aClient) {
    this._connection = aConn;
    this._client = aClient;
    this._host = _connection._host;
    this._port = _connection._port;
  }
  
  public NSBlock Openfile(String aFilename, String aMode) throws NSQLException {
    long version = (NSConnection._blockMeta != null)? NSConnection._blockMeta.version: 0L;
    try {
      TOpenFileReq req = new TOpenFileReq(aFilename, version);
      TOpenFileResp resp = _client.OpenFile(req);
      Utils.verifySuccess(resp.getStatus());
      if (resp.isSetBlkmeta()) {
        NSConnection._blockMeta = resp.blkmeta;
      }
      return new NSBlock(resp.blkId);
    } catch (NSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new NSQLException(e.toString(), e);
    }
  }
  
  public NSResultSet execute(NSBlock aBlk, NSOpType aType, String aInBuff)
      throws NSQLException {
    return execute(aBlk, aType, aInBuff, _defaultRWOffset, _defaultRWSize, null);
  }
  
  public NSResultSet execute(NSBlock aBlk, NSOpType aType, String aInBuff,
      long aOffset, long aSize) throws NSQLException {
    return execute(aBlk, aType, aInBuff, aOffset, aSize, null);
  }
  
  public NSResultSet execute(String aFilename, NSOpType aType, String aInBuff)
      throws NSQLException {
    return execute(null, aType, aInBuff, _defaultRWOffset, _defaultRWSize, aFilename);
  }
  
  public NSResultSet execute(String aFilename, NSOpType aType, String aInBuff,
      long aOffset, long aSize) throws NSQLException {
    return execute(null, aType, aInBuff, aOffset, aSize, aFilename);
  }
  
  public NSResultSet execute(NSBlock aBlk, NSOpType aType, String aInBuff,
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
    op = new TOperation(0L, _connection.getRandOpId(), type);
    op.setRw_param(new TRWparam(aOffset, aSize).setBuffer(aInBuff));
    if (aType == NSOpType.READ || aType == NSOpType.WRITE || aType == NSOpType.UPDATE) {
      CheckReConnect(aBlk, aFilename);
    }
    try {
      TExecuteOperationReq req;
      TExecuteOperationResp resp = null;
      // If REQUESTED_WRONG_NODE is returned, retry 3 times
      for (int i = 0; i < 3; i++) {
        if (aBlk == null && aFilename != null) {
          req = new TExecuteOperationReq("", op, NSConnection._blockMeta.version);
          req.setFile_path(aFilename);
        } else {
          req = new TExecuteOperationReq(aBlk._blkId, op,
            NSConnection._blockMeta.version);
        }
        resp = _client.ExecuteOperation(req);
        if (resp.getStatus().getStatus_code() == TStatusCode.REQUESTED_WRONG_NODE) {
          if (resp.isSetBlkmeta()) {
            NSConnection._blockMeta = resp.blkmeta;
          }
          CheckReConnect(aBlk, aFilename);
        } else {
          break;
        }
      }
      Utils.verifySuccess(resp.getStatus());
      return new NSResultSet(resp.result);
    } catch (NSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new NSQLException(e.toString(), e);
    }
  }
  
  private void CheckReConnect(NSBlock aBlk, String aFilename) throws NSQLException {
    if (NSConnection._blockMeta != null) {
      TLeaderRep laddr = null;
      if (aBlk != null) {
        laddr = NSConnection._blockMeta.blkidleader_map.get(aBlk._blkId);
      } else if (aFilename != null) {
        List<String> blkIds = NSConnection._blockMeta.fileblkid_map
            .get(aFilename);
        if (blkIds != null) {
          laddr = NSConnection._blockMeta.blkidleader_map.get(blkIds.get(0));
        }
      }
      if (laddr != null) {
        if (!(laddr.leader_repaddr.hostname.equals(_host)) ||
            !(laddr.leader_repaddr.rsm_port == _port)) {
          // debug code
          System.err.println("reconnect: preaddr = " + _host + ", postaddr = " +
            laddr.leader_repaddr.hostname);
          //
          //make another connection
          openTransportInOp(laddr.leader_repaddr.hostname,
            laddr.leader_repaddr.rsm_port);
        }
      }
    } else {
      try {
        // update block meta
        TGetCBlockMetaResp getMetaResp =
          _client.GetCBlockMeta(
            new TGetCBlockMetaReq(
              (NSConnection._blockMeta != null)? NSConnection._blockMeta.version: 0L
            )
          );
        Utils.verifySuccess(getMetaResp.getStatus());
        if (getMetaResp.isSetBlkmeta()) {
          NSConnection._blockMeta = getMetaResp.blkmeta;
        }
      } catch (NSQLException e) {
        throw e;
      } catch (Exception e) {
        throw new NSQLException(e.toString(), e);
      }
    }
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
  
  synchronized private void openTransportInOp(String aHost, int aPort) throws NSQLException {
    if (_transport != null) {
      _transport.close();
    }
    _transport = new TSocket(aHost, aPort, _connection._socketTimeout);

    TProtocol protocol = new TBinaryProtocol(_transport);
    _client = new ReplicaService.Client(protocol);
    
    try {
      _transport.open();
    } catch (Exception e) {
      throw new NSQLException("Could not establish connection to " + aHost + ":"
        + aPort + e.getMessage(), e);
    }
    _host = aHost;
    _port = aPort;
  }
}
