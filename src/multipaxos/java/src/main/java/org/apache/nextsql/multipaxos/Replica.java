package org.apache.nextsql.multipaxos;

import org.apache.nextsql.multipaxos.nodemanager.INodeManager;
import org.apache.nextsql.multipaxos.thrift.*;
import org.apache.nextsql.multipaxos.util.SystemInfo;
import org.apache.nextsql.storage.IStorage;
import org.apache.nextsql.storage.StorageException;

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
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class Replica {
  private static final Logger LOG = LoggerFactory.getLogger(Replica.class);
  
  private static ExecutorService _threadPool = new ThreadPoolExecutor(
      2, 36, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
  private boolean _leader = false;
  private final long _blockId;
  private long _replicaId;
  private int _leaderInd;
  protected List<Long> _replicaLocs;
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
  private final IStorage _storage;
  private final Paxos _paxosProtocol;
  protected final INodeManager _nodeMgr;
  
  private class ExecuteDupSlotOp implements Callable {
    private long _blkId;
    private TOperation _op;
    public ExecuteDupSlotOp(long aBlkId, TOperation aOp) {
      this._blkId = aBlkId;
      this._op = aOp;
    }
    @Override
    public Object call() throws Exception {
      return ExecuteOperation(_blkId, _op);
    }
  }
  
  public Replica(long aBlkId, List<Long> aLocs, int aLeaderInd,
      IStorage aStorage, INodeManager aNodeMgr) throws MultiPaxosException {
    this._blockId = aBlkId;
    this._leaderInd = aLeaderInd;
    this._replicaLocs = aLocs;
    this._leader = aLocs.get(aLeaderInd).equals(SystemInfo.getNetworkAddress());
    // TODO: gen replica id
    // this._replicaId = 
    this._storage = aStorage;
    this._nodeMgr = aNodeMgr;
    // replica, leader, acceptor are co-located
    this._paxosProtocol = new Paxos(this, aLocs);
  }
  
  public Paxos getPaxos() {
    return _paxosProtocol;
  }
  
  public TExecuteOperationResp ExecuteOperation(long aBlkId, TOperation aOp)
      throws TException {
    LOG.debug("ExecuteOperation is requested to the replica: blkid = " + aBlkId);
    TExecuteOperationResp resp = new TExecuteOperationResp();
    // check duplicated operation
    if (_decisions.containsValue(aOp)) {
      LOG.error("Duplicated operation is requested to replica");
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
    _proposals.put(newSlotNum, aOp);
    // send accept msg to the leader
    TLeaderAcceptResp acceptResp = null;
    // TODO: read op can request to any replica
    if (_leader) { // || aOp.getOperation_type() == TOpType.OP_READ) {
      acceptResp = _paxosProtocol.LeaderAccept(aBlkId, newSlotNum, aOp);
    } else {
      TNetworkAddress leaderAddr = _nodeMgr.getNode(_replicaLocs.get(_leaderInd));
      TProtocol leaderProtocol = getProtocol(leaderAddr.hostname, leaderAddr.paxos_port);
      leaderProtocol.getTransport().open();
      PaxosService.Iface client = new PaxosService.Client(leaderProtocol);
      if (client != null) {
        acceptResp = client.LeaderAccept(new TLeaderAcceptReq(aBlkId, newSlotNum, aOp));
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
    _decisions.put(newSlotNum, aOp);
    for (TOperation op = _decisions.get(_decisionSlotNum); op != null
        ; op = _decisions.get(_decisionSlotNum)) {
      TOperation pop = _proposals.get(_decisionSlotNum);
      if (pop != null && !pop.equals(op)) {
        // need error handling?
        Set<Future<TExecuteOperationResp>> execOpResps = new HashSet<Future<TExecuteOperationResp>>();
        execOpResps.add(_threadPool.submit(new ExecuteDupSlotOp(aBlkId, pop)));
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
  
  static protected TProtocol getProtocol(String aHostname, int aPort)
      throws TTransportException {
    TTransport sTransport = new TSocket(aHostname, aPort, 0);
    return new TBinaryProtocol(sTransport);
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
          case OP_OPEN:
            break;
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
          case OP_DELETE:
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

  public TDecisionResp Decision(long aBlkId, long aSlotNum, TOperation aOp) throws TException {
    LOG.debug("Decision is requested to the replica");
    TDecisionResp resp = new TDecisionResp();
    _decisions.put(aSlotNum, aOp);
    for (TOperation op = _decisions.get(_decisionSlotNum); op != null
        ; op = _decisions.get(_decisionSlotNum)) {
      TOperation pop = _proposals.get(_decisionSlotNum);
      if (pop != null && !pop.equals(op)) {
        // need error handling?
        // what if it's read op?
        Set<Future<TExecuteOperationResp>> execOpResps = new HashSet<Future<TExecuteOperationResp>>();
        execOpResps.add(_threadPool.submit(new ExecuteDupSlotOp(aBlkId, pop)));
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
