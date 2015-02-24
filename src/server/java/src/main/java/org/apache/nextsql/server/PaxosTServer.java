package org.apache.nextsql.server;

import org.apache.nextsql.multipaxos.Replica;
import org.apache.nextsql.multipaxos.thrift.PaxosService;
import org.apache.nextsql.multipaxos.thrift.TAcceptorPhaseOneReq;
import org.apache.nextsql.multipaxos.thrift.TAcceptorPhaseOneResp;
import org.apache.nextsql.multipaxos.thrift.TAcceptorPhaseTwoReq;
import org.apache.nextsql.multipaxos.thrift.TAcceptorPhaseTwoResp;
import org.apache.nextsql.multipaxos.thrift.TExecuteOperationResp;
import org.apache.nextsql.multipaxos.thrift.THeartbeatResp;
import org.apache.nextsql.multipaxos.thrift.TLeaderAcceptReq;
import org.apache.nextsql.multipaxos.thrift.TLeaderAcceptResp;
import org.apache.nextsql.multipaxos.thrift.TLeaderProposeReq;
import org.apache.nextsql.multipaxos.thrift.TLeaderProposeResp;
import org.apache.nextsql.multipaxos.thrift.TStatus;
import org.apache.nextsql.multipaxos.thrift.TStatusCode;
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
    Replica rep = _blkMgr.getReplicafromBlkID(aReq.blockId);
    if (rep == null) {
      LOG.error("The blockId(" + aReq.blockId + "is unknown");
      TAcceptorPhaseOneResp resp =
        new TAcceptorPhaseOneResp(new TStatus(TStatusCode.ERROR), null, null);
      resp.getStatus().setError_message("The file(" + aReq.blockId + "is unknown");
      return resp;
    }
    return null;
  }

  @Override
  public TAcceptorPhaseTwoResp AcceptorPhaseTwo(TAcceptorPhaseTwoReq aReq)
      throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public THeartbeatResp Heartbeat() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TLeaderAcceptResp LeaderAccept(TLeaderAcceptReq aReq)
      throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TLeaderProposeResp LeaderPropose(TLeaderProposeReq aReq)
      throws TException {
    // TODO Auto-generated method stub
    return null;
  }
}
