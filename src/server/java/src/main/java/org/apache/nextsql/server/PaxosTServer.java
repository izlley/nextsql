package org.apache.nextsql.server;

import org.apache.nextsql.multipaxos.Replica;
import org.apache.nextsql.thrift.PaxosService;
import org.apache.nextsql.thrift.TAcceptorPhaseOneReq;
import org.apache.nextsql.thrift.TAcceptorPhaseOneResp;
import org.apache.nextsql.thrift.TAcceptorPhaseTwoReq;
import org.apache.nextsql.thrift.TAcceptorPhaseTwoResp;
import org.apache.nextsql.thrift.THeartbeatResp;
import org.apache.nextsql.thrift.TLeaderAcceptReq;
import org.apache.nextsql.thrift.TLeaderAcceptResp;
import org.apache.nextsql.thrift.TLeaderProposeReq;
import org.apache.nextsql.thrift.TLeaderProposeResp;
import org.apache.nextsql.thrift.TStatus;
import org.apache.nextsql.thrift.TStatusCode;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PaxosTServer implements PaxosService.Iface {
  private static final Logger LOG = LoggerFactory.getLogger(PaxosTServer.class);
  private final BlockManager _blkMgr;
  
  PaxosTServer(BlockManager aBlkMgr) {
    this._blkMgr = aBlkMgr;
  }

  @Override
  public TAcceptorPhaseOneResp AcceptorPhaseOne(TAcceptorPhaseOneReq aReq)
      throws TException {
    TAcceptorPhaseOneResp resp;
    Replica rep = _blkMgr.getReplicafromRepId(aReq.repId);
    if (rep == null) {
      LOG.error("The replicaId(" + aReq.repId + "is unknown");
      resp = new TAcceptorPhaseOneResp(new TStatus(TStatusCode.ERROR), null, null);
      resp.getStatus().setError_message("The replicaId(" + aReq.repId + "is unknown");
      return resp;
    }
    return rep.getPaxos().AcceptorPhaseOne(aReq.ballot_num);
  }

  @Override
  public TAcceptorPhaseTwoResp AcceptorPhaseTwo(TAcceptorPhaseTwoReq aReq)
      throws TException {
    TAcceptorPhaseTwoResp resp;
    Replica rep = _blkMgr.getReplicafromRepId(aReq.repId);
    if (rep == null) {
      LOG.error("The replicaId(" + aReq.repId + "is unknown");
      resp = new TAcceptorPhaseTwoResp(new TStatus(TStatusCode.ERROR), null);
      resp.getStatus().setError_message("The replicaId(" + aReq.repId + "is unknown");
      return resp;
    }
    return rep.getPaxos().AcceptorPhaseTwo(aReq.ballot_num, aReq.slot_num, aReq.operation);
  }

  @Override
  public THeartbeatResp Heartbeat() throws TException {
    THeartbeatResp resp = new THeartbeatResp(new TStatus(TStatusCode.SUCCESS));
    return resp;
  }

  @Override
  public TLeaderAcceptResp LeaderAccept(TLeaderAcceptReq aReq)
      throws TException {
    TLeaderAcceptResp resp;
    Replica rep = _blkMgr.getReplicafromRepId(aReq.repId);
    if (rep == null) {
      LOG.error("The replicaId(" + aReq.repId + "is unknown");
      resp = new TLeaderAcceptResp(new TStatus(TStatusCode.ERROR));
      resp.getStatus().setError_message("The replicaId(" + aReq.repId + "is unknown");
      return resp;
    }
    return rep.getPaxos().LeaderAccept(aReq.slot_num, aReq.operation);
  }

  @Override
  public TLeaderProposeResp LeaderPropose(TLeaderProposeReq aReq)
      throws TException {
    Replica rep = _blkMgr.getReplicafromRepId(aReq.repId);
    if (rep == null) {
      LOG.error("The replicaId(" + aReq.repId + "is unknown");
      TLeaderProposeResp resp =
        new TLeaderProposeResp(new TStatus(TStatusCode.ERROR), null);
      resp.getStatus().setError_message("The replicaId(" + aReq.repId + "is unknown");
      return resp;
    }
    return rep.getPaxos().LeaderPropose(aReq.ballot_num);
  }
}
