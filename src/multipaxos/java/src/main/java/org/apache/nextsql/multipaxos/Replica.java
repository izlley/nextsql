package org.apache.nextsql.multipaxos;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
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
import org.apache.nextsql.common.NextSqlException;
import org.apache.nextsql.multipaxos.blockmanager.IBlockManager;
import org.apache.nextsql.multipaxos.nodemanager.INodeManager;
import org.apache.nextsql.storage.IStorage;

import org.apache.nextsql.thrift.PaxosService;
import org.apache.nextsql.thrift.TDecisionResp;
import org.apache.nextsql.thrift.TExecResult;
import org.apache.nextsql.thrift.TExecuteOperationResp;
import org.apache.nextsql.thrift.TLeaderAcceptReq;
import org.apache.nextsql.thrift.TLeaderAcceptResp;
import org.apache.nextsql.thrift.TNetworkAddress;
import org.apache.nextsql.thrift.TOperation;
import org.apache.nextsql.thrift.TRepNode;
import org.apache.nextsql.thrift.TStatus;
import org.apache.nextsql.thrift.TStatusCode;

public class Replica {
  private static final Logger LOG = LoggerFactory.getLogger(Replica.class);
  
  private static ExecutorService _threadPool = new ThreadPoolExecutor(
      2, 36, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
  private boolean _leader = false;
  protected final String _blockId;
  protected final String _replicaId;
  protected int _leaderIdx;
  protected List<TRepNode> _replicaNodes;
  
  // need more think...
  public static enum State {
    ACTIVE,
    RECOVERY,
    CLOSED
  }
  private State _state = State.ACTIVE;
  
  /*
   * fields for MultiPaxos
   */
  private AtomicLong _slotNum = new AtomicLong(0);
  private AtomicLong _decisionSlotNum = new AtomicLong(1);
  private ConcurrentHashMap<Long, TOperation> _proposals =
    new ConcurrentHashMap<Long, TOperation>();
  private ConcurrentHashMap<Long, TOperation> _decisions =
    new ConcurrentHashMap<Long, TOperation>();
  private final Executor _executor;
  private final Paxos _paxosProtocol;
  protected final INodeManager _nodeMgr;
  protected final IBlockManager _blockMgr;
  protected final IStorage _storage;
  
  private class ExecuteDupSlotOp implements Callable {
    private TOperation _op;
    public ExecuteDupSlotOp(TOperation aOp) {
      this._op = aOp;
    }
    @Override
    public Object call() throws Exception {
      return ExecuteOperation(_op);
    }
  }
  
  public Replica(String aBlkId, String aRepId, List<TRepNode> aReps, int aLeaderIdx,
      IStorage aStorage, IBlockManager aBlkMgr, INodeManager aNodeMgr, boolean aIsLeader)
        throws NextSqlException {
    this._blockId = aBlkId;
    this._replicaId = aRepId;
    this._leaderIdx = aLeaderIdx;
    this._replicaNodes = aReps;
    this._leader = aIsLeader;
    this._nodeMgr = aNodeMgr;
    this._blockMgr = aBlkMgr;
    this._storage = aStorage;
    this._executor = new Executor(this);
    // replica, leader, acceptor are co-located
    this._paxosProtocol = new Paxos(this, aReps, aIsLeader);
  }
  
  public Paxos getPaxos() {
    return _paxosProtocol;
  }
  
  private boolean containsSameOp(ConcurrentHashMap<Long, TOperation> map, TOperation aOp) {
    for (Entry<Long, TOperation> en: _decisions.entrySet()) {
      if (en.getValue().operation_handle == aOp.operation_handle) {
        return true;
      }
    }
    return false;
  }
  
  static protected TProtocol getProtocol(String aHostname, int aPort)
      throws TTransportException {
    TTransport sTransport = new TSocket(aHostname, aPort, 0);
    return new TBinaryProtocol(sTransport);
  }
  
  public TExecuteOperationResp ExecuteOperation(TOperation aOp)
      throws NextSqlException {
    LOG.debug("ExecuteOperation is requested to the replica: repid = " + _replicaId);
    TExecuteOperationResp resp = new TExecuteOperationResp();
    // check duplicated operation in the decisions map
    if (containsSameOp(_decisions, aOp)) {
      LOG.error("Duplicated operation is requested to replica");
      throw new NextSqlException("Duplicated operation is requested to replica");
    }
    ///////////////////
    // Propose phase
    ///////////////////
    // assign slotNum
    long newSlotNum = _slotNum.incrementAndGet();
    // add to proposals map
    _proposals.put(newSlotNum, aOp);
    LOG.debug("The replica add new (slot,op) in the proposalMap: SN = {}, Op = {}:{}",
      newSlotNum, aOp.operation_handle, aOp.operation_type, aOp);
    LOG.debug("Current proposalMap entries :{}", _proposals.toString());
    // send accept msg to the leader
    TLeaderAcceptResp acceptResp = null;
    // TODO: read op can be requested to any replica
    if (_leader) { // || (aOp.getOperation_type() == TOpType.OP_READ)) {
      acceptResp = _paxosProtocol.LeaderAccept(newSlotNum, aOp);
    } else {
      TRepNode lRepNode = _replicaNodes.get(_leaderIdx);
      TNetworkAddress leaderAddr = _nodeMgr.getNodeInfo(lRepNode.node_id);
      try {
        TProtocol leaderProtocol = getProtocol(leaderAddr.hostname, leaderAddr.paxos_port);
        leaderProtocol.getTransport().open();
        PaxosService.Iface client = new PaxosService.Client(leaderProtocol);
        acceptResp = client.LeaderAccept(
          new TLeaderAcceptReq(lRepNode.replica_id, newSlotNum, aOp)
        );
        leaderProtocol.getTransport().close();
      } catch (TException e) {
        LOG.error("Thrift protocol error to leader: ", e);
        resp.setStatus(new TStatus(TStatusCode.ERROR));
        resp.getStatus().setError_message("Thrift protocol error to leader.");
        return resp;
      }
    }
    if (acceptResp.getStatus().getStatus_code() != TStatusCode.SUCCESS) {
      resp.setStatus(acceptResp.getStatus());
      return resp;
    }
    resp.setResult(acceptResp.exec_result);
    resp.setStatus(new TStatus(TStatusCode.SUCCESS));
    return resp;
  }
  
  private void perform(TOperation aOp, TExecResult aRes) throws NextSqlException {
    // check duplicated operation
    for (Entry<Long, TOperation> entry: _decisions.entrySet()) {
      if (entry.getKey() < _decisionSlotNum.get() &&
          entry.getValue().operation_handle == aOp.operation_handle) {
        LOG.debug("Skip performing duplicated operation. preSN = {}, currSN = {}",
          entry.getKey(), _decisionSlotNum.get());
        _decisionSlotNum.incrementAndGet();
        return;
      }
    }
    _executor.exec(aOp.operation_handle,
                   aOp.operation_type,
                   aOp.getDdl_param(),
                   aOp.getRw_param(),
                   aRes);
  }

  public TDecisionResp Decision(long aSlotNum, TOperation aOp)
      throws TException {
    LOG.debug("Decision is requested to the replica");
    TDecisionResp resp = new TDecisionResp();
    if (aSlotNum > _slotNum.get()) {
      synchronized(this) {
        if (aSlotNum > _slotNum.get()) {
          _slotNum.set(aSlotNum);
        }
      }
    }
    _decisions.put(aSlotNum, aOp);
    LOG.debug("The replica add new (slot,op) in the decisionMap: SN = {}, Op = {}:{}",
      aSlotNum, aOp.operation_handle, aOp.operation_type, aOp);
    LOG.debug("Current decisionMap entries :{}", _decisions.toString());
    List<Future<TExecuteOperationResp>> execOpResps =
      new ArrayList<Future<TExecuteOperationResp>>();
    // TODO : I don't know this works properly
    // execute operations in sequential order
    for (TOperation op = _decisions.get(_decisionSlotNum.get()); op != null
        ; op = _decisions.get(_decisionSlotNum.get())) {
      TOperation pop = _proposals.get(_decisionSlotNum.get());
      if (pop != null && !pop.equals(op)) {
        // duplicated slotnum in proposal map should be assigned a new slotnum
        // what if it's a read op?
        execOpResps.add(_threadPool.submit(new ExecuteDupSlotOp(pop)));
      }
      // execute the operation
      try {
        TExecResult result = new TExecResult();
        perform(op, result);
        long curSlot = _decisionSlotNum.getAndIncrement();
        // only leader returns the operation result value
        if (_leader) { // && curSlot == aSlotNum) {
          resp.setResult(result);
        } // else is lagging case?
      } catch (NextSqlException e) {
        resp.setStatus(new TStatus(TStatusCode.ERROR));
        resp.getStatus().setError_message("Storage IO failure: " + e.getMessage());
        return resp;
      }
    }
    resp.setStatus(new TStatus(TStatusCode.SUCCESS));
    return resp;
  }
}
