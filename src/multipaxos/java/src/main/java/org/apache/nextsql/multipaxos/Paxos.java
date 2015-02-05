package org.apache.nextsql.multipaxos;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.nextsql.multipaxos.thrift.*;
import org.apache.thrift.TException;
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
      // send p2a msg to acceptors
      
      _replica.getProtocol(aLoc);
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
