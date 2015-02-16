package org.apache.nextsql.multipaxos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.nextsql.multipaxos.thrift.*;
import org.apache.nextsql.multipaxos.util.SystemInfo;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Paxos implements PaxosService.Iface {
  private static final Logger LOG = LoggerFactory.getLogger(Paxos.class);
  
  private static ExecutorService _threadPool = new ThreadPoolExecutor(
      8, 36, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
  private Replica _replica = null;
  private Leader _leader = null;
  private Acceptor _acceptor = null;
  protected List<TNetworkAddress> _acceptorLocs;
  
  private static enum PaxosMsgType {
    P1A, P2A
  }
  
  private class SendPaxosMsg implements Callable {
    private PaxosMsgType _type;
    private TNetworkAddress _dest;
    private Object _msg;
    private boolean _self;
    public SendPaxosMsg(PaxosMsgType aType, TNetworkAddress aAddr, Object aMsg, boolean aSelf) {
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
          TAcceptorPhaseOneResp resp = null;
          if (_self) {
            resp = AcceptorPhaseOne((TAcceptorPhaseOneReq)_msg);
          } else {
            TProtocol acceptorProtocol = Replica.getProtocol(_dest);
            acceptorProtocol.getTransport().open();
            PaxosService.Iface client = new PaxosService.Client(acceptorProtocol);
            if (client != null) {
              resp = client.AcceptorPhaseOne((TAcceptorPhaseOneReq)_msg);
            }// else exception
            acceptorProtocol.getTransport().close();
          }
          return resp;
        }
        case P2A:
        {
          TAcceptorPhaseTwoResp resp = null;
          if (_self) {
            resp = AcceptorPhaseTwo((TAcceptorPhaseTwoReq)_msg);
          } else {
            TProtocol acceptorProtocol = Replica.getProtocol(_dest);
            acceptorProtocol.getTransport().open();
            PaxosService.Iface client = new PaxosService.Client(acceptorProtocol);
            if (client != null) {
              resp = client.AcceptorPhaseTwo((TAcceptorPhaseTwoReq)_msg);
            }// else exception
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
    public SendDecisionMsg(TNetworkAddress aAddr, TDecisionReq aMsg) {
      this._dest = aAddr;
      this._msg = aMsg;
    }
    @Override
    public Object call() throws Exception {
      TDecisionResp resp = null;
      TProtocol replicaProtocol = Replica.getProtocol(_dest);
      replicaProtocol.getTransport().open();
      ReplicaService.Iface client = new ReplicaService.Client(replicaProtocol);
      if (client != null) {
        resp = client.Decision(_msg);
      }// else exception
      replicaProtocol.getTransport().close();
      return resp;
    }
  }
  
  public Paxos(Replica aReplica, List<TNetworkAddress> aLocs) throws MultiPaxosException {
    this._replica = aReplica;
    this._acceptorLocs = aLocs;
    this._leader = new Leader();
    this._acceptor = new Acceptor();
  }

  @Override
  public TLeaderProposeResp LeaderPropose(TLeaderProposeReq aReq)
      throws TException {
    LOG.debug("LeaderPropose is requested.");
    TLeaderProposeResp resp = new TLeaderProposeResp();
    if (compareBallotNums(aReq.getBallot_num(), _leader.getBallotNum()) > 0) {
      _leader.setBallotNum(aReq.getBallot_num());
      try {
        proposeAndAdopt(aReq.getBallot_num());
      } catch (TException e) {
        LOG.error("LeaderPropose failure: " + e.getMessage());
        resp.getStatus().setStatus_code(TStatusCode.ERROR);
        resp.setBallot_num(_leader.getBallotNum());
        resp.getStatus().setError_message("LeaderPropose failure: " + e.getMessage());
        return resp;
      }
      resp.getStatus().setStatus_code(TStatusCode.SUCCESS);
    } else {
      LOG.error("LeaderPropose failure: the requested BN is smaller than leader's BN");
      resp.setStatus(new TStatus(TStatusCode.ERROR));
      resp.setBallot_num(_leader.getBallotNum());
      resp.getStatus().setError_message("LeaderPropose failure: the requested BN is smaller than leader's BN");
    }
    return resp;
  }

  @Override
  public TLeaderAcceptResp LeaderAccept(TLeaderAcceptReq aReq) throws TException {
    LOG.debug("LeaderAccept is requested");
    TLeaderAcceptResp resp = new TLeaderAcceptResp();
    // check duplicated operation
    if (_leader._proposals.containsKey(aReq.getSlot_num())) {
      resp.setStatus(new TStatus(TStatusCode.ERROR));
      resp.getStatus().setError_message("Slot_num has already occupied at leader");
      return resp;
    }
    // add to leader's proposals map
    _leader._proposals.put(aReq.getSlot_num(), aReq.getOperation());
    if (_leader._active.get()) {
      acceptAndDecide(aReq.getSlot_num(), aReq.getOperation());
    }
    resp.setStatus(new TStatus(TStatusCode.SUCCESS));
    return resp;
  }
  
  private void acceptAndDecide(long aSlotNum, TOperation aOp) throws TException {
    TAcceptorPhaseTwoReq p2aReq = new TAcceptorPhaseTwoReq(_leader.getBallotNum(),
        aSlotNum, aOp);
    Set<Future<TAcceptorPhaseTwoResp>> p2bResps = new HashSet<Future<TAcceptorPhaseTwoResp>>();
    // send p2a msg to acceptors
    for (TNetworkAddress acc: _acceptorLocs) {
      LOG.debug("Leader send p2a msg to aceptor(" + acc.getHostname() + ")");
      if (acc.equals(SystemInfo.getNetworkAddress())) {
        p2bResps.add(_threadPool.submit(new SendPaxosMsg(PaxosMsgType.P2A, acc, p2aReq, true)));
      } else {
        p2bResps.add(_threadPool.submit(new SendPaxosMsg(PaxosMsgType.P2A, acc, p2aReq, false)));
      }
    }
    int accCnt = _acceptorLocs.size();
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
              if (p2bResp.getBallot_num().equals(p2aReq.getBallot_num())) {
                ++completeCnt;
                it.remove();
              } else {
                LOG.info("Leader enter preempted mode. reqBN = " + 
                  p2aReq.getBallot_num().id + ":" + p2aReq.getBallot_num().getProposer().hostname +
                  ", respBN = " + p2bResp.getBallot_num().id + ":" + p2bResp.getBallot_num().getProposer().hostname);
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
    if (preempted && p2bResp != null) {
      // another leader is elected?
      TAcceptorPhaseOneResp p1aResp = preempted(p2bResp.getBallot_num());
    } else {
      TDecisionReq decisionReq = new TDecisionReq(aSlotNum, aOp);
      Set<Future<TDecisionResp>> decisionResps = new HashSet<Future<TDecisionResp>>();
      // send a decision msg to remote replicas
      for (TNetworkAddress replicaLoc: _replica._replicaLocs) {
        if (!replicaLoc.equals(SystemInfo.getNetworkAddress())) {
          decisionResps.add(_threadPool.submit(new SendDecisionMsg(replicaLoc, decisionReq)));
        }
      }
    }
  }
  
  private TAcceptorPhaseOneResp preempted(TBallotNum aBallotNum) throws TException {
    TAcceptorPhaseOneResp p1aResp = null;
    if (compareBallotNums(aBallotNum, _leader.getBallotNum()) > 0) {
      _leader._active.set(false);
      _leader.increaseAndGetBN();
      LOG.info("Leader increase the BN to " + _leader.getBallotNum().id + ":" +
        _leader.getBallotNum().getProposer().hostname);
      proposeAndAdopt(_leader.getBallotNum());
    }
    return p1aResp;
  }
  
  // propose the new ballot
  private void proposeAndAdopt(TBallotNum aBallotNum) throws TException {
    TAcceptorPhaseOneReq p1aReq = new TAcceptorPhaseOneReq(aBallotNum);
    Set<Future<TAcceptorPhaseOneResp>> p1bResps = new HashSet<Future<TAcceptorPhaseOneResp>>();
    // send p1a msg to acceptors
    for (TNetworkAddress acc: _acceptorLocs) {
      LOG.debug("Leader send p1a msg to aceptor(" + acc.getHostname() + ")");
      if (acc.equals(SystemInfo.getNetworkAddress())) {
        p1bResps.add(_threadPool.submit(new SendPaxosMsg(PaxosMsgType.P1A, acc, p1aReq, true)));
      } else {
        p1bResps.add(_threadPool.submit(new SendPaxosMsg(PaxosMsgType.P1A, acc, p1aReq, false)));
      }
    }
    int accCnt = _acceptorLocs.size();
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
              if (p1bResp.getBallot_num().equals(p1aReq.getBallot_num())) {
                for (TAcceptedValue e: p1bResp.getAccepted_values()) {
                  if (!pvalues.containsKey(e.slot_num)) {
                    pvalues.put(e.slot_num, e);
                  } else {
                    TAcceptedValue old = pvalues.get(e.slot_num);
                    if (compareBallotNums(e.getBallot_num(), old.getBallot_num()) > 0) {
                      pvalues.put(e.slot_num, e);
                    }
                  }
                }
                ++completeCnt;
                it.remove();
              } else {
                LOG.info("Leader enter preempted mode. reqBN = " + 
                  p1aReq.getBallot_num().id + ":" + p1aReq.getBallot_num().getProposer().hostname +
                  ", respBN = " + p1bResp.getBallot_num().id + ":" + p1bResp.getBallot_num().getProposer().hostname);
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
    if (preempted && p1bResp != null) {
      // another leader is elected?
      TAcceptorPhaseOneResp p1aResp = preempted(p1bResp.getBallot_num());
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
            LOG.debug("Leader try to adopt pre-values: SN = " + slotNum);
            acceptAndDecide(slotNum, _leader._proposals.get(slotNum));
          } catch (TException e) {
            LOG.error("Adopting a pre-value failed: " + e.getMessage());
          }
        }
      }
      _leader._active.set(true);
    }
  }

  @Override
  public TAcceptorPhaseOneResp AcceptorPhaseOne(TAcceptorPhaseOneReq aReq)
      throws TException {
    TBallotNum bn = _acceptor.getBallotNum();
    if (compareBallotNums(aReq.getBallot_num(), bn) > 0) {
      _acceptor.setBallotNum(aReq.getBallot_num());
      bn = aReq.getBallot_num();
    }
    return new TAcceptorPhaseOneResp(bn, _acceptor.getAcceptVals());
  }

  @Override
  public TAcceptorPhaseTwoResp AcceptorPhaseTwo(TAcceptorPhaseTwoReq aReq)
      throws TException {
    TBallotNum bn = _acceptor.getBallotNum();
    if (compareBallotNums(aReq.getBallot_num(), bn) >= 0) {
      _acceptor.setBallotNum(aReq.getBallot_num());
      _acceptor.addAcceptVal(
        new TAcceptedValue(aReq.getBallot_num(), aReq.getSlot_num(), aReq.getOperation()));
      bn = aReq.getBallot_num();
    }
    return new TAcceptorPhaseTwoResp(bn);
  }

  @Override
  public THeartbeatResp Heartbeat() throws TException {
    return null;
  }

  // if bn1 is greater than bn2, then return 1
  // if bn1 is equal to bn2, then return 0
  // if bn1 is less than bn2, than return -1
  static private int compareBallotNums(TBallotNum aBn1, TBallotNum aBn2) {
    if (aBn1.getId() == aBn2.getId() &&
        aBn1.getProposer().hostname.compareTo(aBn2.getProposer().hostname) == 0) {
      return 0;
    } else if ((aBn1.getId() > aBn2.getId()) || (aBn1.getId() == aBn2.getId() &&
        (aBn1.getProposer().hostname.compareTo(aBn2.getProposer().hostname) > 0))
      ) {
      return 1;
    } else {
      return -1;
    }
  }
}
