package org.apache.nextsql.multipaxos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.nextsql.common.NextSqlException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.nextsql.multipaxos.util.SystemInfo;
import org.apache.nextsql.thrift.PaxosService;
import org.apache.nextsql.thrift.ReplicaService;
import org.apache.nextsql.thrift.TAcceptedValue;
import org.apache.nextsql.thrift.TAcceptorPhaseTwoReq;
import org.apache.nextsql.thrift.TAcceptorPhaseTwoResp;
import org.apache.nextsql.thrift.TBallotNum;
import org.apache.nextsql.thrift.TDecisionReq;
import org.apache.nextsql.thrift.TDecisionResp;
import org.apache.nextsql.thrift.TExecResult;
import org.apache.nextsql.thrift.THeartbeatResp;
import org.apache.nextsql.thrift.TLeaderAcceptResp;
import org.apache.nextsql.thrift.TLeaderProposeResp;
import org.apache.nextsql.thrift.TNetworkAddress;
import org.apache.nextsql.thrift.TAcceptorPhaseOneReq;
import org.apache.nextsql.thrift.TAcceptorPhaseOneResp;
import org.apache.nextsql.thrift.TOperation;
import org.apache.nextsql.thrift.TRepNode;
import org.apache.nextsql.thrift.TStatus;
import org.apache.nextsql.thrift.TStatusCode;

public class Paxos {
  private static final Logger LOG = LoggerFactory.getLogger(Paxos.class);
  
  private static ExecutorService _threadPool = new ThreadPoolExecutor(
      8, 36, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
  private Replica _replica = null;
  private Leader _leader = null;
  private Acceptor _acceptor = null;
  protected List<TRepNode> _acceptorNodes;
  
  private static enum PaxosMsgType {
    P1A, P2A
  }
  
  private class SendPaxosMsg implements Callable {
    private PaxosMsgType _type;
    private TNetworkAddress _dest;
    private Object _msg;
    private boolean _self;
    public SendPaxosMsg(PaxosMsgType aType, TNetworkAddress aAddr, Object aMsg,
        boolean aSelf) {
      this._type = aType;
      this._dest = aAddr;
      this._msg = aMsg;
      this._self = aSelf;
    }
    @Override
    public Object call() throws Exception {
      switch (_type) {
        case P1A:
        {
          TAcceptorPhaseOneReq req = (TAcceptorPhaseOneReq)_msg;
          TAcceptorPhaseOneResp resp = null;
          if (_self) {
            resp = AcceptorPhaseOne(req.ballot_num);
          } else {
            TProtocol acceptorProtocol = Replica.getProtocol(_dest.hostname, _dest.paxos_port);
            acceptorProtocol.getTransport().open();
            PaxosService.Iface client = new PaxosService.Client(acceptorProtocol);
            resp = client.AcceptorPhaseOne((TAcceptorPhaseOneReq)_msg);
            acceptorProtocol.getTransport().close();
          }
          return resp;
        }
        case P2A:
        {
          TAcceptorPhaseTwoReq req = (TAcceptorPhaseTwoReq)_msg;
          TAcceptorPhaseTwoResp resp = null;
          if (_self) {
            resp = AcceptorPhaseTwo(req.ballot_num, req.slot_num, req.operation);
          } else {
            TProtocol acceptorProtocol = Replica.getProtocol(_dest.hostname, _dest.paxos_port);
            acceptorProtocol.getTransport().open();
            PaxosService.Iface client = new PaxosService.Client(acceptorProtocol);
            resp = client.AcceptorPhaseTwo((TAcceptorPhaseTwoReq)_msg);
            acceptorProtocol.getTransport().close();
          }
          return resp;
        }
        default:
          LOG.error("unknown paxos message type.");
          return null;
      }
    }
  }
  
  private class SendDecisionMsg implements Callable {
    private TNetworkAddress _dest;
    private TDecisionReq _msg;
    private boolean _leader;
    public SendDecisionMsg(TNetworkAddress aAddr, TDecisionReq aMsg, boolean aLeader) {
      this._dest = aAddr;
      this._msg = aMsg;
      this._leader = aLeader;
    }
    @Override
    public Object call() throws Exception {
      TDecisionResp resp = null;
      if (_leader) {
        resp = _replica.Decision(_msg.slot_num, _msg.operation);
      } else {
        TProtocol replicaProtocol = Replica.getProtocol(_dest.hostname,
          _dest.rsm_port);
        replicaProtocol.getTransport().open();
        ReplicaService.Iface client = new ReplicaService.Client(replicaProtocol);
        resp = client.Decision(_msg);
        replicaProtocol.getTransport().close();
      }
      return resp;
    }
  }
  
  public Paxos(Replica aReplica, List<TRepNode> aReps, boolean aIsLeader)
      throws NextSqlException {
    this._replica = aReplica;
    this._acceptorNodes = aReps;
    this._leader =
      new Leader(
        new TBallotNum(
          0L, aReplica._nodeMgr.getNodeId(SystemInfo.getNetworkAddress())
        ),
        aIsLeader
      );
    this._acceptor = new Acceptor();
  }

  public TLeaderProposeResp LeaderPropose(TBallotNum aBn) {
    LOG.debug("LeaderPropose is requested.");
    TLeaderProposeResp resp = new TLeaderProposeResp();
    TBallotNum lbn = _leader.getBallotNumClone();
    if (compareBallotNums(aBn, lbn) > 0) {
      // TODO: need logging to stable storage
      _leader.setBallotNum(aBn);
      try {
        proposeAndAdopt(aBn);
      } catch (NextSqlException e) {
        resp.setStatus(new TStatus(TStatusCode.ERROR));
        resp.getStatus().setError_message(e.getMessage());
        return resp;
      }
      resp.getStatus().setStatus_code(TStatusCode.SUCCESS);
    } else {
      final String errmsg =
        "LeaderPropose failure: the requested BN is smaller than leader's BN";
      LOG.error(errmsg);
      resp.setStatus(new TStatus(TStatusCode.ERROR));
      resp.setBallot_num(lbn);
      resp.getStatus().setError_message(errmsg);
    }
    return resp;
  }

  public TLeaderAcceptResp LeaderAccept(long aSlotNum, TOperation aOp) {
    LOG.debug("LeaderAccept is requested");
    TLeaderAcceptResp resp = new TLeaderAcceptResp();
    // check the requested slot is already proposed
    if (_leader._proposals.containsKey(aSlotNum)) {
      LOG.debug("Slot_num has already occupied at leader");
      resp.setStatus(new TStatus(TStatusCode.ERROR));
      resp.getStatus().setError_message("Slot_num has already occupied at leader");
      return resp;
    }
    // add to leader's proposals map
    _leader._proposals.put(aSlotNum, aOp);
    if (_leader._active.get()) {
      try {
        TExecResult result = acceptAndDecide(aSlotNum, aOp);
        resp.setExec_result(result);
      } catch (NextSqlException e) {
        resp.setStatus(new TStatus(TStatusCode.ERROR));
        resp.getStatus().setError_message(e.getMessage());
        return resp;
      }
    }
    resp.setStatus(new TStatus(TStatusCode.SUCCESS));
    return resp;
  }
  
  private TExecResult acceptAndDecide(long aSlotNum, TOperation aOp) 
      throws NextSqlException {
    TExecResult result = null;
    int accCnt = _acceptorNodes.size();
    List<Future<TAcceptorPhaseTwoResp>> p2bResps =
      new ArrayList<Future<TAcceptorPhaseTwoResp>>(accCnt);
    TBallotNum lbn = _leader.getBallotNumClone();
    // send p2a msg to acceptors
    for (TRepNode acc: _acceptorNodes) {
      TNetworkAddress accAddr = _replica._nodeMgr.getNodeInfo(acc.node_id);
      TAcceptorPhaseTwoReq p2aReq =
        new TAcceptorPhaseTwoReq(acc.replica_id, lbn, aSlotNum, aOp);
      if (accAddr == null) {
        LOG.error("There is no Acceptor node mapping to the ID. (ID = {})",
          acc.node_id);
        continue;
      }
      LOG.debug("Leader send p2a msg to aceptor({}:{})",
        accAddr.hostname, accAddr.paxos_port);
      p2bResps.add(
        _threadPool.submit(
          new SendPaxosMsg(PaxosMsgType.P2A,
                           accAddr,
                           p2aReq,
                           (acc.node_id == _replica._nodeMgr.getMyNodeId()))
        )
      );
    }
    int completeCnt = 0;
    boolean preempted = false;
    TAcceptorPhaseTwoResp p2bResp = null;
    // loop until majority accepted
    while (completeCnt <= accCnt/2 && !preempted) {
      for (Iterator<Future<TAcceptorPhaseTwoResp>> it = p2bResps.iterator(); it.hasNext();) {
        Future<TAcceptorPhaseTwoResp> res = it.next();
        if (res.isDone() && !res.isCancelled()) {
          try {
            p2bResp = res.get();
            if (p2bResp != null) {
              LOG.debug("Leader get p2b msg from aceptor");
              if (p2bResp.getBallot_num().equals(lbn)) {
                ++completeCnt;
                it.remove();
              } else {
                LOG.info("Leader enter preempted mode. reqBN = {}:{}, respBN = {}:{}",
                  lbn.id, lbn.nodeid, p2bResp.getBallot_num().id, p2bResp.getBallot_num().nodeid);
                preempted = true;
                break;
              }
            }
          } catch (InterruptedException e) {
            // ignore
          } catch (ExecutionException e) {
            LOG.error("P2A sender failure: ", e);
          }
        }
      }
    }
    if (preempted) {
      // another leader is elected?
      preempted(p2bResp.getBallot_num());
    } else {
      List<Future<TDecisionResp>> decisionResps =
        new ArrayList<Future<TDecisionResp>>(_replica._replicaNodes.size());
      // send a decision msg to all replicas
      int i = 0;
      for (TRepNode rep: _replica._replicaNodes) {
        TDecisionReq decisionReq =
          new TDecisionReq(rep.replica_id, aSlotNum, aOp);
        TNetworkAddress repAddr = _replica._nodeMgr.getNodeInfo(rep.node_id);
        decisionResps.add(
          _threadPool.submit(
            new SendDecisionMsg(
              repAddr, decisionReq, (i++ == _replica._leaderIdx))
          )
        );
      }
      try {
        // wait till leader commit process is completed, because the leader
        // must have up-to-dated objects
        TDecisionResp resp = decisionResps.get(_replica._leaderIdx).get();
        result = resp.result;
      } catch (InterruptedException e) {
        // ignore
      } catch (ExecutionException e) {
        LOG.error("Leader commit failed: ", e);
        throw new NextSqlException("Leader commit failed: " + e.getMessage());
      }
    }
    return result;
  }
  
  private void preempted(TBallotNum aBallotNum) throws NextSqlException {
    TBallotNum lbn = _leader.getBallotNumClone();
    if (compareBallotNums(aBallotNum, lbn) > 0) {
      _leader._active.set(false);
      // TODO : need timewait to avoid infinite contention to be a leader or 
      //        we need leader lease mechanism
      _leader.increaseAndGetBN();
      LOG.info("Leader increase the BN to {}:{}", lbn.id, lbn.nodeid);
      // init phase1 to be a leader
      proposeAndAdopt(lbn);
    }
  }
  
  // propose the new ballot
  private void proposeAndAdopt(TBallotNum aBallotNum) throws NextSqlException {
    int accCnt = _acceptorNodes.size();
    List<Future<TAcceptorPhaseOneResp>> p1bResps =
      new ArrayList<Future<TAcceptorPhaseOneResp>>(accCnt);
    // send p1a msg to acceptors
    for (TRepNode acc: _acceptorNodes) {
      TAcceptorPhaseOneReq p1aReq = new TAcceptorPhaseOneReq(acc.replica_id, aBallotNum);
      TNetworkAddress accAddr = _replica._nodeMgr.getNodeInfo(acc.node_id);
      if (accAddr == null) {
        LOG.error("There is no Acceptor node mapping to the ID. (ID = " +
          acc.node_id + ")");
        continue;
      }
      LOG.debug("Leader send p1a msg to aceptor(" + accAddr.getHostname() + ")");
      p1bResps.add(
        _threadPool.submit(
          new SendPaxosMsg(PaxosMsgType.P1A,
                           accAddr,
                           p1aReq,
                           (acc.node_id == _replica._nodeMgr.getMyNodeId()))
        )
      );
    }
    int completeCnt = 0;
    boolean preempted = false;
    TAcceptorPhaseOneResp p1bResp = null;
    Map<Long, TAcceptedValue> pvalues = new HashMap<Long, TAcceptedValue>();
    // loop until majority accepted
    while (completeCnt <= accCnt/2 && !preempted) {
      for (Iterator<Future<TAcceptorPhaseOneResp>> it = p1bResps.iterator(); it.hasNext();) {
        Future<TAcceptorPhaseOneResp> res = it.next();
        if (res.isDone() && !res.isCancelled()) {
          try {
            p1bResp = res.get();
            if (p1bResp != null) {
              LOG.debug("Leader get p1b msg from aceptor");
              if (p1bResp.getBallot_num().equals(aBallotNum)) {
                for (TAcceptedValue e: p1bResp.getAccepted_values()) {
                  if (!pvalues.containsKey(e.slot_num)) {
                    pvalues.put(e.slot_num, e);
                  } else {
                    TAcceptedValue old = pvalues.get(e.slot_num);
                    // if slotnum is same, choose the latest bn
                    if (compareBallotNums(e.getBallot_num(), old.getBallot_num()) > 0) {
                      pvalues.put(e.slot_num, e);
                    }
                  }
                }
                ++completeCnt;
                it.remove();
              } else {
                LOG.info("Leader enters preempted mode. reqBN = {}:{}, respBN = {}:{}", 
                  aBallotNum.id, aBallotNum.nodeid, p1bResp.getBallot_num().id,
                  p1bResp.getBallot_num().nodeid);
                preempted = true;
                break;
              }
            }
          } catch (InterruptedException e) {
            // ignore
          } catch (ExecutionException e) {
            LOG.error("P2A sender failure:" + e.getCause());
          }
        }
      }
    }
    if (preempted) {
      // another leader is elected?
      preempted(p1bResp.getBallot_num());
    } else {
      // update proposals map
      for (TAcceptedValue e: pvalues.values()) {
        _leader._proposals.put(e.slot_num, e.operation);
      }
      // adopt pre-values
      if (!_leader._proposals.isEmpty()) {
        List<Long> sorted = new ArrayList<Long>(_leader._proposals.keySet());
        Collections.sort(sorted);
        for (long slotNum : sorted) {
          try {
            LOG.debug("Leader try to adopt pre-values: SN = {}", slotNum);
            acceptAndDecide(slotNum, _leader._proposals.get(slotNum));
          } catch (NextSqlException e) {
            LOG.error("Adopting a pre-value failed: SN = {}, {}", slotNum,
              e.getMessage());
            throw e;
          }
        }
      }
      _leader._active.set(true);
    }
  }

  public TAcceptorPhaseOneResp AcceptorPhaseOne(TBallotNum aBn) {
    LOG.debug("AcceptorPhaseOne is requested in Paxos: BN = {}:{}",
      aBn.id, aBn.nodeid);
    TBallotNum bn = _acceptor.getBallotNumClone();
    if (compareBallotNums(aBn, bn) > 0) {
      // TODO: need logging to stable storage
      _acceptor.setBallotNum(aBn);
      LOG.debug("Acceptor's BN is updated on P1A: from = {}:{}, to = {}:{}",
        bn.id, bn.nodeid, aBn.id, aBn.nodeid);
      bn = aBn;
    }
    TAcceptorPhaseOneResp resp =
      new TAcceptorPhaseOneResp(new TStatus(TStatusCode.SUCCESS), bn,
        _acceptor.getAcceptVals());
    return resp;
  }

  public TAcceptorPhaseTwoResp AcceptorPhaseTwo(TBallotNum aBn, long aSlotNum,
      TOperation aOp) throws TException {
    LOG.debug("AcceptorPhaseTwo is requested in Paxos: BN = {}:{}, SN = {}",
      aBn.id, aBn.nodeid, aSlotNum);
    TBallotNum bn =_acceptor.getBallotNumClone();
    if (compareBallotNums(aBn, bn) >= 0) {
      // TODO: need logging to stable storage
      _acceptor.setBallotNum(aBn);
      _acceptor.addAcceptVal(
        new TAcceptedValue(aBn, aSlotNum, aOp));
      LOG.debug("Acceptor accept the requested value on P2A: BN = {}:{}, SN = {}, OP = {}:{}",
        aBn.id, aBn.nodeid, aSlotNum, aOp.operation_handle, aOp.getOperation_type());
      LOG.debug("Current acceptMap entries :{}", _acceptor.getAcceptVals().toString());
      bn = aBn;
    }
    TAcceptorPhaseTwoResp resp =
      new TAcceptorPhaseTwoResp(new TStatus(TStatusCode.SUCCESS), bn);
    return resp;
  }

  // Does this need replica-level heartbeat
  public THeartbeatResp Heartbeat() {
    return null;
  }

  // if bn1 is greater than bn2, then return 1
  // if bn1 is equal to bn2, then return 0
  // if bn1 is less than bn2, than return -1
  static private int compareBallotNums(TBallotNum aBn1, TBallotNum aBn2) {
    int res;
    if (aBn1 == null || aBn2 == null) {
      if (aBn1 == null && aBn2 == null) {
        res = 0;
      } else if (aBn2 == null) {
        res = 1;
      } else {
        res = -1;
      }
    } else {
      if ((aBn1.getId() == aBn2.getId() && aBn1.getNodeid() == aBn2.getNodeid())) {
        res = 0;
      } else if ((aBn1.getId() > aBn2.getId())
        || (aBn1.getId() == aBn2.getId() && (aBn1.getNodeid() > aBn2.getNodeid()))) {
        res = 1;
      } else {
        res = -1;
      }
    }
    return res;
  }
}
