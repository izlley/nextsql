package org.apache.nextsql.multipaxos;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nextsql.multipaxos.thrift.*;
import org.apache.nextsql.util.SystemInfo;
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
  private List<TNetworkAddress> _acceptorLocs;
  
  private class SendP2AMsg implements Callable {
    private TNetworkAddress _dest;
    private TAcceptorPhaseTwoReq _msg;
    private boolean _self;
    public SendP2AMsg(TNetworkAddress aAddr, TAcceptorPhaseTwoReq aMsg, boolean aSelf) {
      this._dest = aAddr;
      this._msg = aMsg;
      this._self = aSelf;
    }
    @Override
    public Object call() throws Exception {
      TAcceptorPhaseTwoResp resp = null;
      if (_self) {
        resp = AcceptorPhaseTwo(_msg);
      } else {
        TProtocol acceptorProtocol = Replica.getProtocol(_dest);
        acceptorProtocol.getTransport().open();
        PaxosService.Iface client = new PaxosService.Client(acceptorProtocol);
        if (client != null) {
          resp = client.AcceptorPhaseTwo(_msg);
        }// else exception
        acceptorProtocol.getTransport().close();
      }
      return resp;
    }
  }
  
  private class SendP1AMsg implements Callable {
    private TNetworkAddress _dest;
    private TAcceptorPhaseTwoReq _msg;
    private boolean _self;
    public SendP1AMsg(TNetworkAddress aAddr, TAcceptorPhaseTwoReq aMsg, boolean aSelf) {
      this._dest = aAddr;
      this._msg = aMsg;
      this._self = aSelf;
    }
    @Override
    public Object call() throws Exception {
      TAcceptorPhaseTwoResp resp = null;
      if (_self) {
        resp = AcceptorPhaseTwo(_msg);
      } else {
        TProtocol acceptorProtocol = Replica.getProtocol(_dest);
        acceptorProtocol.getTransport().open();
        PaxosService.Iface client = new PaxosService.Client(acceptorProtocol);
        if (client != null) {
          resp = client.AcceptorPhaseTwo(_msg);
        }// else exception
        acceptorProtocol.getTransport().close();
      }
      return resp;
    }
  }
  
  private class SendDecisionMsg implements Callable {
    private TNetworkAddress _dest;
    private TAcceptorPhaseTwoReq _msg;
    private boolean _self;
    public SendP1AMsg(TNetworkAddress aAddr, TAcceptorPhaseTwoReq aMsg, boolean aSelf) {
      this._dest = aAddr;
      this._msg = aMsg;
      this._self = aSelf;
    }
    @Override
    public Object call() throws Exception {
      TAcceptorPhaseTwoResp resp = null;
      if (_self) {
        resp = AcceptorPhaseTwo(_msg);
      } else {
        TProtocol acceptorProtocol = Replica.getProtocol(_dest);
        acceptorProtocol.getTransport().open();
        PaxosService.Iface client = new PaxosService.Client(acceptorProtocol);
        if (client != null) {
          resp = client.AcceptorPhaseTwo(_msg);
        }// else exception
        acceptorProtocol.getTransport().close();
      }
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
  public TLeaderProposeResp LeaderPropose(TLeaderProposeReq req)
      throws TException {
    return null;
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
    if (_leader._active) {
      TAcceptorPhaseTwoReq p2aReq = new TAcceptorPhaseTwoReq(_leader._ballotNum,
          aReq.getSlot_num(), aReq.getOperation());
      Set<Future<TAcceptorPhaseTwoResp>> p2bResps = new HashSet<Future<TAcceptorPhaseTwoResp>>();
      // send p2a msg to acceptors
      for (TNetworkAddress acc : _acceptorLocs) {
        if (acc.equals(SystemInfo.getNetworkAddress())) {
          p2bResps.add(_threadPool.submit(new SendP2AMsg(acc, p2aReq, true)));
        } else {
          p2bResps.add(_threadPool.submit(new SendP2AMsg(acc, p2aReq, false)));
        }
      }
      int accCnt = _acceptorLocs.size();
      int completeCnt = 0;
      boolean preempted = false;
      // loop until majority accepted
      while (completeCnt <= accCnt/2 && !preempted) {
        for (Iterator<Future<TAcceptorPhaseTwoResp>> it = p2bResps.iterator(); it.hasNext();) {
          Future<TAcceptorPhaseTwoResp> res = it.next();
          if (res.isDone() && !res.isCancelled()) {
            TAcceptorPhaseTwoResp p2bResp;
            try {
              p2bResp = res.get();
              if (p2bResp != null) {
                if (p2bResp.getBallot_num().equals(p2aReq.getBallot_num())) {
                  ++completeCnt;
                  it.remove();
                } else {
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
        // init phase one again
        
      } else {
        // send a decision msg to replicas
        
      }
    }
    return null;
  }

  @Override
  public TAcceptorPhaseOneResp AcceptorPhaseOne(TAcceptorPhaseOneReq req)
      throws TException {
    return null;
  }

  @Override
  public TAcceptorPhaseTwoResp AcceptorPhaseTwo(TAcceptorPhaseTwoReq req)
      throws TException {
    return null;
  }

  @Override
  public THeartbeatResp Heartbeat() throws TException {
    return null;
  }

}
