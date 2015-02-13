package org.apache.nextsql.multipaxos;

import org.apache.nextsql.multipaxos.thrift.*;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
  
  private static ExecutorService _threadPool = new ThreadPoolExecutor(
      2, 36, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
  private boolean _leader = false;
  private long _blockid;
  private TNetworkAddress _leaderLoc;
  protected List<TNetworkAddress> _replicaLocs;
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
  private ConcurrentHashMap<Long, TOperation> _proposals = new ConcurrentHashMap<Long, TOperation>();
  private ConcurrentHashMap<Long, TOperation> _decisions = new ConcurrentHashMap<Long, TOperation>();
  private IStorage _storage = null;
  private Paxos _paxosProtocol = null;
  
  private class ExecuteDupSlotOp implements Callable {
    private TExecuteOperationReq _param;
    public ExecuteDupSlotOp(TExecuteOperationReq aParam) {
      this._param = aParam;
    }
    @Override
    public Object call() throws Exception {
      return ExecuteOperation(_param);
    }
  }
  
  public Replica(long aBlkId, List<TNetworkAddress> aLocs, boolean aLeader,
      TNetworkAddress aLeaderAddr, IStorage aStorage) throws MultiPaxosException {
    this._blockid = aBlkId;
    this._leaderLoc = aLeaderAddr;
    this._replicaLocs = aLocs;
    this._leader = aLeader;
    this._storage = aStorage;
    // replica, leader, acceptor are co-located
    this._paxosProtocol = new Paxos(this, aLocs);
  }
  
  @Override
  public TExecuteOperationResp ExecuteOperation(TExecuteOperationReq aReq)
      throws TException {
    LOG.debug("ExecuteOperation is requested to the replica");
    TExecuteOperationResp resp = new TExecuteOperationResp();
    // check duplicated operation
    if (_decisions.containsValue(aReq.getOperation())) {
      resp.setStatus(new TStatus(TStatusCode.ERROR));
      resp.getStatus().setError_message("Duplicated operation is requested to replica");
      return resp;
    }
    ///////////////////
    // Propose phase
    ///////////////////
    // assign slotNum
    long newSlotNum = _slotNum.incrementAndGet();
    LOG.debug("The replica propose a new SN = " + newSlotNum);
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
      leaderProtocol.getTransport().close();
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
        // need error handling?
        TExecuteOperationReq execOpReq = new TExecuteOperationReq(pop);
        Set<Future<TExecuteOperationResp>> execOpResps = new HashSet<Future<TExecuteOperationResp>>();
        execOpResps.add(_threadPool.submit(new ExecuteDupSlotOp(execOpReq)));
      }
      // execute the operation
      try {
        resp.setData(perform(op));
      } catch (StorageException e) {
        resp.setStatus(new TStatus(TStatusCode.ERROR));
        resp.getStatus().setError_message("Storage IO failure: " + e.getMessage());
        return resp;
      }
    }
    resp.setStatus(new TStatus(TStatusCode.SUCCESS));
    return resp;
  }
  
  static protected TProtocol getProtocol(TNetworkAddress aLoc)
      throws TTransportException {
    TTransport sTransport = new TSocket(aLoc.hostname, aLoc.port, 0);
    return new TCompactProtocol(sTransport);
  }
  
  private String perform(TOperation op) throws StorageException {
    String result = null;
    // check duplicated operation
    for (Map.Entry<Long, TOperation> entry: _decisions.entrySet()) {
      if (entry.getKey() < _decisionSlotNum.get() && entry.getValue().equals(op)) {
        LOG.debug("Skip performing duplicated operation. preSN = " + entry.getKey() +
          ", currSN = " + _decisionSlotNum.get());
        _decisionSlotNum.incrementAndGet();
        return result;
      }
    }
    // do operation
    int retry = 3;
    synchronized(this) {
      LOG.debug("Try to exec the operation: type = " + op.getOperation_type() + ", data = " + op.data);
      for (; retry > 0; --retry) {
        switch (op.getOperation_type()) {
          case OP_READ:
            byte[] readbuf = new byte[(int) op.size];
            try {
              _storage.read(readbuf, op.offset, op.size);
              retry = 0;
            } catch (StorageException e) {
              LOG.error("Read operation failed: " + e.getMessage());
              throw e;
            }
            // we need to eliminate memcpys
            result = new String(readbuf, Charset.forName("UTF-8"));
            break;
          case OP_WRITE:
            try {
              _storage.write(op.data.getBytes(Charset.forName("UTF-8")),
                  op.offset, op.size);
              retry = 0;
            } catch (StorageException e) {
              LOG.error("Write operation failed" + e.getMessage());
              throw e;
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
      _decisionSlotNum.incrementAndGet();
    }
    return result;
  }

  @Override
  public TDecisionResp Decision(TDecisionReq aReq) throws TException {
    LOG.debug("Decision is requested to the replica");
    TDecisionResp resp = new TDecisionResp();
    _decisions.put(aReq.getSlot_num(), aReq.getOperation());
    for (TOperation op = _decisions.get(_decisionSlotNum); op != null
        ; op = _decisions.get(_decisionSlotNum)) {
      TOperation pop = _proposals.get(_decisionSlotNum);
      if (pop != null && !pop.equals(op)) {
        // need error handling?
        TExecuteOperationReq execOpReq = new TExecuteOperationReq(pop);
        Set<Future<TExecuteOperationResp>> execOpResps = new HashSet<Future<TExecuteOperationResp>>();
        execOpResps.add(_threadPool.submit(new ExecuteDupSlotOp(execOpReq)));
      }
      // execute the operation
      try {
        perform(op);
      } catch (StorageException e) {
        resp.setStatus(new TStatus(TStatusCode.ERROR));
        resp.getStatus().setError_message("Storage IO failure: " + e.getMessage());
        return resp;
      }
    }
    resp.setStatus(new TStatus(TStatusCode.SUCCESS));
    return resp;
  }
}
