package org.apache.nextsql.multipaxos;

import org.apache.nextsql.multipaxos.thrift.*;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.nextsql.storage.IStorage;
import org.apache.nextsql.storage.StorageException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class Replica implements ReplicaService.Iface {
  private static final Logger LOG = LoggerFactory.getLogger(Replica.class);
  private boolean _leader = false;
  private long _blockid;
  private TNetworkAddress _leaderLoc;
  private List<TNetworkAddress> _locations;
  private long _size = 0;
  
  // need more think...
  public static enum State {
    ACTIVE,
    RECOVERY,
    CLOSED
  }
  private State _state = State.ACTIVE;
  
  // must be atomic?
  private AtomicLong _slotNum = new AtomicLong(0);
  private AtomicLong _decisionSlotNum = new AtomicLong(1);
  private HashMap<Long, TOperation> _proposals = new HashMap<Long, TOperation>();
  private HashMap<Long, TOperation> _decisions = new HashMap<Long, TOperation>();
  private IStorage _storage = null;
  private Paxos _paxosProtocol = null;
  
  public Replica(long aBlkId, List<TNetworkAddress> aLocs, boolean aLeader,
      TNetworkAddress aLeaderAddr, IStorage aStorage) {
    this._blockid = aBlkId;
    this._leaderLoc = aLeaderAddr;
    this._locations = aLocs;
    this._leader = aLeader;
    this._storage = aStorage;
    this._paxosProtocol = new Paxos(this);
  }
  
  @Override
  public TExecuteOperationResp ExecuteOperation(TExecuteOperationReq aReq)
      throws TException {
    LOG.debug("ExecuteOperation is requested");
    TExecuteOperationResp resp = new TExecuteOperationResp();
    // check duplicated operation
    if (_decisions.containsValue(aReq.getOperation())) {
      resp.setStatus(new TStatus(TStatusCode.ERROR));
      resp.getStatus().setError_message("Duplicated operation is requested");
      return resp;
    }
    ///////////////////
    // Propose phase
    ///////////////////
    // assign slotNum
    long newSlotNum = _slotNum.incrementAndGet();
    // add to proposals map
    _proposals.put(newSlotNum, aReq.getOperation());
    // send accept msg to the leader
    TLeaderAcceptReq acceptReq = new TLeaderAcceptReq(newSlotNum,
        aReq.getOperation());
    TLeaderAcceptResp acceptResp = null;
    if (_leader) {
      acceptResp = _paxosProtocol.LeaderAccept(acceptReq);
    } else {
      TProtocol leaderProtocol = getProtocol(_leaderLoc);
      leaderProtocol.getTransport().open();
      PaxosService.Iface client = new PaxosService.Client(leaderProtocol);
      if (client != null) {
        acceptResp = client.LeaderAccept(acceptReq);
      }// else exception
    }
    if (acceptResp.getStatus().getStatus_code() != TStatusCode.SUCCESS) {
      resp.setStatus(acceptResp.getStatus());
      return resp;
    }
    ////////////////////
    // Decision phase
    ////////////////////
    _decisions.put(newSlotNum, aReq.getOperation());
    for (TOperation op = _decisions.get(_decisionSlotNum); op != null
        ; op = _decisions.get(_decisionSlotNum)) {
      TOperation pop = _proposals.get(_decisionSlotNum);
      if (pop != null && !pop.equals(op)) {
        // need error handling
        TExecuteOperationResp resp2 =
          ExecuteOperation(new TExecuteOperationReq().setOperation(pop));
      }
      // execute the operation
      perform(op, resp);
    }
    resp.setStatus(new TStatus(TStatusCode.SUCCESS));
    return resp;
  }
  
  private TProtocol getProtocol(TNetworkAddress aLoc)
      throws TTransportException {
    TTransport sTransport = new TSocket(aLoc.hostname, aLoc.port, 0);
    return new TCompactProtocol(sTransport);
  }
  
  private void perform(TOperation op, TExecuteOperationResp result) {
    // check duplicated operation
    for (Map.Entry<Long, TOperation> entry: _decisions.entrySet()) {
      if (entry.getKey() < _decisionSlotNum.get() && entry.getValue().equals(op)) {
        _decisionSlotNum.incrementAndGet();
        return;
      }
    }
    // do operation
    int retry = 3;
    synchronized(this) {
      for (; retry > 0; --retry) {
        switch (op.getOperation_type()) {
          case OP_READ:
            byte[] readbuf = new byte[(int) op.size];
            try {
              _storage.read(readbuf, op.offset, op.size);
              retry = 0;
            } catch (StorageException e) {
              LOG.error("Read operation failed: " + e.getMessage());
              break;
            }
            // we need to eliminate memcpys
            result.data = new String(readbuf, Charset.forName("UTF-8"));
            break;
          case OP_WRITE:
            try {
              _storage.write(op.data.getBytes(Charset.forName("UTF-8")),
                  op.offset, op.size);
              retry = 0;
            } catch (StorageException e) {
              LOG.error("Write operation failed" + e.getMessage());
            }
            break;
          case OP_UPDATE:
          case OP_GETMETA:
          case OP_SETMETA:
          default:
            retry = 0;
            break;
        }
      }
      if (retry == 0) {
        result.setStatus(new TStatus(TStatusCode.ERROR));
        result.getStatus().setError_message("Storage I/O failure");
      } else {
        _decisionSlotNum.incrementAndGet();
      }
    }
  }
}
