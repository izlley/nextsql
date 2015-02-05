package org.apache.nextsql.multipaxos;

import org.apache.nextsql.multipaxos.thrift.*;
import org.apache.thrift.TException;

public class Paxos implements PaxosService.Iface {
  
  private Replica _replica = null;
  private Leader _leader = null;
  private Acceptor _acceptor = null;
  
  public Paxos(Replica aReplica) {
    this._replica = aReplica;
    this._leader = new Leader();
    this._acceptor = new Acceptor();
  }

  @Override
  public TLeaderProposeResp LeaderPropose(TLeaderProposeReq req)
      throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TLeaderAcceptResp LeaderAccept(TLeaderAcceptReq req) throws TException {
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
