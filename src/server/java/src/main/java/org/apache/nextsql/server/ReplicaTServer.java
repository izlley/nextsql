package org.apache.nextsql.server;

import org.apache.nextsql.multipaxos.Replica;
import org.apache.nextsql.thrift.ReplicaService;
import org.apache.nextsql.thrift.TDecisionReq;
import org.apache.nextsql.thrift.TDecisionResp;
import org.apache.nextsql.thrift.TDeleteFileReq;
import org.apache.nextsql.thrift.TDeleteFileResp;
import org.apache.nextsql.thrift.TExecuteOperationReq;
import org.apache.nextsql.thrift.TExecuteOperationResp;
import org.apache.nextsql.thrift.TOpenFileReq;
import org.apache.nextsql.thrift.TOpenFileResp;
import org.apache.nextsql.thrift.TStatus;
import org.apache.nextsql.thrift.TStatusCode;
import org.apache.nextsql.server.BlockManager.PBMVal;
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
  public TOpenFileResp OpenFile(TOpenFileReq aReq) throws TException {
    PBMVal blk = _blkMgr.getBlkfromPath(aReq.file_path);
    if (blk == null) {
      // create a new block
      
    }
    TOpenFileResp resp = new TOpenFileResp(new TStatus(), blk._blkId);
    return resp;
  }
  
  @Override
  public TDeleteFileResp DeleteFile(TDeleteFileReq aReq) throws TException {
    return null;
  }
  
  @Override
  public TExecuteOperationResp ExecuteOperation(TExecuteOperationReq aReq)
      throws TException {
    PBMVal blk = _blkMgr.getBlkfromPath(aReq.file_path);
    Replica rep = _blkMgr.getReplicafromBlkID(blk._blkId);
    if (rep == null) {
      LOG.error("The file(" + aReq.file_path + "is unknown");
      TExecuteOperationResp resp = new TExecuteOperationResp(new TStatus(TStatusCode.ERROR));
      resp.getStatus().setError_message("The file(" + aReq.file_path + "is unknown");
      return resp;
    }
    return rep.ExecuteOperation(blk._blkId, aReq.operation);
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
