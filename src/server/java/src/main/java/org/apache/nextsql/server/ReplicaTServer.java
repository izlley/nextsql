package org.apache.nextsql.server;

import java.security.SecureRandom;

import org.apache.nextsql.multipaxos.Replica;
import org.apache.nextsql.thrift.ReplicaService;
import org.apache.nextsql.thrift.TDecisionReq;
import org.apache.nextsql.thrift.TDecisionResp;
import org.apache.nextsql.thrift.TDeleteFileReq;
import org.apache.nextsql.thrift.TDeleteFileResp;
import org.apache.nextsql.thrift.TExecuteOperationReq;
import org.apache.nextsql.thrift.TExecuteOperationResp;
import org.apache.nextsql.thrift.TOpType;
import org.apache.nextsql.thrift.TOpenFileReq;
import org.apache.nextsql.thrift.TOpenFileResp;
import org.apache.nextsql.thrift.TOperation;
import org.apache.nextsql.thrift.TStatus;
import org.apache.nextsql.thrift.TStatusCode;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaTServer implements ReplicaService.Iface {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicaTServer.class);
  private final BlockManager _blkMgr;
  private final SecureRandom _rand;
  
  ReplicaTServer(BlockManager aBlkMgr) {
    this._blkMgr = aBlkMgr;
    this._rand = new SecureRandom();
  }

  @Override
  public TOpenFileResp OpenFile(TOpenFileReq aReq) throws TException {
    TOpenFileResp resp = new TOpenFileResp();
    Replica rep = _blkMgr.getReplicafromBlkID(1L);
    if (rep == null) {
      LOG.error("Replica is corrupted.");
      resp.setStatus(new TStatus(TStatusCode.ERROR));
      resp.getStatus().setError_message("Replica is corrupted.");
      return resp;
    }
    TOperation op = new TOperation(0L, _rand.nextLong(), TOpType.OP_OPEN, aReq.file_path);
    TExecuteOperationResp openResult = rep.ExecuteOperation(1L, op);
    if (openResult.getStatus().getStatus_code() == TStatusCode.SUCCESS) {
      resp.setBlockId(Long.parseLong(openResult.data));
    }
    return resp.setStatus(openResult.status);
  }
  
  @Override
  public TDeleteFileResp DeleteFile(TDeleteFileReq aReq) throws TException {
    TDeleteFileResp resp = new TDeleteFileResp();
    Replica rep = _blkMgr.getReplicafromBlkID(1L);
    if (rep == null) {
      LOG.error("Replica is corrupted.");
      resp.setStatus(new TStatus(TStatusCode.ERROR));
      resp.getStatus().setError_message("Replica is corrupted.");
      return resp;
    }
    TOperation op = new TOperation(0L, _rand.nextLong(), TOpType.OP_DELETE, aReq.file_path);
    TExecuteOperationResp delResult = rep.ExecuteOperation(1L, op);
    return resp.setStatus(delResult.status);
  }
  
  @Override
  public TExecuteOperationResp ExecuteOperation(TExecuteOperationReq aReq)
      throws TException {
    Replica rep = _blkMgr.getReplicafromPath(aReq.file_path);
    if (rep == null) {
      LOG.error("The file(" + aReq.file_path + "is unknown");
      TExecuteOperationResp resp = new TExecuteOperationResp(new TStatus(TStatusCode.ERROR));
      resp.getStatus().setError_message("The file(" + aReq.file_path + "is unknown");
      return resp;
    }
    return rep.ExecuteOperation(rep.getBlkId(), aReq.operation);
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
