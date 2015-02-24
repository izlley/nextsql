package org.apache.nextsql.server;

import org.apache.nextsql.multipaxos.Replica;
import org.apache.nextsql.multipaxos.thrift.ReplicaService;
import org.apache.nextsql.multipaxos.thrift.TDecisionReq;
import org.apache.nextsql.multipaxos.thrift.TDecisionResp;
import org.apache.nextsql.multipaxos.thrift.TExecuteOperationReq;
import org.apache.nextsql.multipaxos.thrift.TExecuteOperationResp;
import org.apache.nextsql.multipaxos.thrift.TStatus;
import org.apache.nextsql.multipaxos.thrift.TStatusCode;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaTServer implements ReplicaService.Iface {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicaTServer.class);
  private final BlockManager _blkMgr;
  
  ReplicaTServer(BlockManager aBlkMgr) {
    this._blkMgr = aBlkMgr;
  }

  @Override
  public TExecuteOperationResp ExecuteOperation(TExecuteOperationReq aReq)
      throws TException {
    Long blkId = _blkMgr.getBlkIDfromPath(aReq.file_path);
    Replica rep = _blkMgr.getReplicafromBlkID(blkId);
    if (rep == null) {
      LOG.error("The file(" + aReq.file_path + "is unknown");
      TExecuteOperationResp resp = new TExecuteOperationResp(new TStatus(TStatusCode.ERROR));
      resp.getStatus().setError_message("The file(" + aReq.file_path + "is unknown");
      return resp;
    }
    return rep.ExecuteOperation(blkId, aReq.operation);
  }
  
  @Override
  public TDecisionResp Decision(TDecisionReq aReq) throws TException {
    Replica rep = _blkMgr.getReplicafromBlkID(aReq.blockId);
    if (rep == null) {
      LOG.error("The blockId(" + aReq.blockId + "is unknown");
      TDecisionResp resp = new TDecisionResp(new TStatus(TStatusCode.ERROR));
      resp.getStatus().setError_message("The blockId(" + aReq.blockId + "is unknown");
      return resp;
    }
    return rep.Decision(aReq.blockId, aReq.slot_num, aReq.operation);
  }
}
