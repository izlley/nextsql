package org.apache.nextsql.server;

import java.util.List;
import org.apache.nextsql.common.NextSqlException;
import org.apache.nextsql.multipaxos.Replica;
import org.apache.nextsql.thrift.ReplicaService;
import org.apache.nextsql.thrift.TDDLparam;
import org.apache.nextsql.thrift.TDecisionReq;
import org.apache.nextsql.thrift.TDecisionResp;
import org.apache.nextsql.thrift.TDeleteFileReq;
import org.apache.nextsql.thrift.TDeleteFileResp;
import org.apache.nextsql.thrift.TExecuteOperationReq;
import org.apache.nextsql.thrift.TExecuteOperationResp;
import org.apache.nextsql.thrift.TGetCBlockMetaReq;
import org.apache.nextsql.thrift.TGetCBlockMetaResp;
import org.apache.nextsql.thrift.TOpType;
import org.apache.nextsql.thrift.TOpenFileReq;
import org.apache.nextsql.thrift.TOpenFileResp;
import org.apache.nextsql.thrift.TOperation;
import org.apache.nextsql.thrift.TRepNode;
import org.apache.nextsql.thrift.TStatus;
import org.apache.nextsql.thrift.TStatusCode;
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
    LOG.debug("OpenFile is requested: " + aReq.file_path);
    TOpenFileResp resp = new TOpenFileResp();
    List<String> blockIds = _blkMgr.getBlkIdsfromPath(aReq.file_path);
    if (blockIds != null) {
      resp.setBlkId(blockIds.get(0));
    } else {
      //
      // create a new file
      //
      // 1. get "DDL" Paxos group's local repId
      try {
        List<String> repIds = _blkMgr.getLocalRepIdfromBlkId("DDL");
        if (repIds.size() == 0) {
          LOG.error("There is no replicaId for DDL at this node.");
          throw new NextSqlException(
            "There is no replicaId for DDL at this node."
          );
        }
        Replica rep = null;
        rep = _blkMgr.getReplicafromRepId(repIds.get(0));
        if (rep == null) {
          LOG.error("Replica is corrupted.");
          throw new NextSqlException(
            "Replica is corrupted."
          );
        }
        // 2. alloc blockId, {nodeId, repId}s
        String blkId = RandomIdGenerator.getNewBlockId();
        List<TRepNode> repNodes = _blkMgr.getRepNodes(blkId);

        // 3. send to Paxos protocol
        TOperation op = new TOperation(0L, RandomIdGenerator.getNewOperationId(),
          TOpType.OP_OPEN);
        // TODO: It would be useful to choose storage type in this method
        // struct TDDLparam { ... 4: optional i16 storage_type }
        op.setDdl_param(
          new TDDLparam(aReq.file_path).setReplicas(repNodes).setBlock_id(blkId)
        );

        TExecuteOperationResp openResult = rep.ExecuteOperation(op);
        if (openResult.getStatus().getStatus_code() != TStatusCode.SUCCESS) {
          throw new NextSqlException(openResult.getStatus().error_message);
        }
        resp.setBlkId(blkId);
      } catch (NextSqlException e) {
        LOG.error(e.getMessage(), e);
        resp.setStatus(new TStatus(TStatusCode.ERROR));
        resp.getStatus().setError_message(e.getMessage());
        return resp;
      }
    }
    if (aReq.blkmeta_version < _blkMgr.getCBlkMeta().version) {
      resp.setBlkmeta(_blkMgr.getCBlkMeta());
    }
    resp.setStatus(new TStatus(TStatusCode.SUCCESS));
    return resp;
  }
  
  @Override
  public TDeleteFileResp DeleteFile(TDeleteFileReq aReq) throws TException {
    LOG.debug("DeleteFile is requested: " + aReq.file_path);
    TDeleteFileResp resp = new TDeleteFileResp();
    try {
      List<String> blockIds = _blkMgr.getBlkIdsfromPath(aReq.file_path);
      if (blockIds == null) {
        throw new NextSqlException(
          "The file does not exist. ( " + aReq.file_path + " )"
        );
      }
      // 1. get "DDL" Paxos group's local repId
      List<String> repIds = _blkMgr.getLocalRepIdfromBlkId("DDL");
      if (repIds.size() == 0) {
        LOG.error("There is no replicaId for DDL at this node.");
        throw new NextSqlException(
          "There is no replicaId for DDL at this node."
        );
      }
      Replica rep = null;
      rep = _blkMgr.getReplicafromRepId(repIds.get(0));
      if (rep == null) {
        LOG.error("Replica is corrupted.");
        throw new NextSqlException("Replica is corrupted.");
      }
      // 3. send to Paxos protocol
      TOperation op = new TOperation(0L, RandomIdGenerator.getNewOperationId(),
        TOpType.OP_DELETE);
      op.setDdl_param(new TDDLparam(aReq.file_path));

      TExecuteOperationResp openResult = rep.ExecuteOperation(op);
      if (openResult.getStatus().getStatus_code() != TStatusCode.SUCCESS) {
        throw new NextSqlException(openResult.getStatus().error_message);
      }
    } catch (NextSqlException e) {
      LOG.error(e.getMessage(), e);
      resp.setStatus(new TStatus(TStatusCode.ERROR));
      resp.getStatus().setError_message(e.getMessage());
      return resp;
    }
    if (aReq.blkmeta_version < _blkMgr.getCBlkMeta().version) {
      resp.setBlkmeta(_blkMgr.getCBlkMeta());
    }
    resp.setStatus(new TStatus(TStatusCode.SUCCESS));
    return resp;
  }
  
  @Override
  public TExecuteOperationResp ExecuteOperation(TExecuteOperationReq aReq)
      throws TException {
    LOG.debug("ExecuteOperation is requested: blkId = " + aReq.blkId +
      ", filePath = " + aReq.file_path);
    TExecuteOperationResp resp;
    Replica rep = null;
    try {
      if (aReq.isSetFile_path()) {
        if (!_blkMgr.isFileExists(aReq.file_path)) {
          throw new NextSqlException("The file ( " + aReq.file_path
            + " ) does not exist");
        }
        rep = _blkMgr.getLocalReplica(aReq.file_path, null);
      } else {
        rep = _blkMgr.getLocalReplica(null, aReq.blkId);
      }
      if (rep == null) {
        resp = new TExecuteOperationResp(new TStatus(TStatusCode.REQUESTED_WRONG_NODE));
        if (aReq.blkmeta_version < _blkMgr.getCBlkMeta().version) {
          resp.setBlkmeta(_blkMgr.getCBlkMeta());
        }
        return resp;
      } else {
        resp = rep.ExecuteOperation(aReq.operation);
      }
    } catch (NextSqlException e) {
      LOG.error(e.getMessage(), e);
      resp = new TExecuteOperationResp(new TStatus(TStatusCode.ERROR));
      resp.getStatus().setError_message(e.getMessage());
      return resp;
    }
    return resp;
  }
  
  @Override
  public TDecisionResp Decision(TDecisionReq aReq) throws TException {
    Replica rep = _blkMgr.getReplicafromRepId(aReq.repid);
    if (rep == null) {
      LOG.error("The replicaId ( " + aReq.repid + " ) is unknown");
      TDecisionResp resp = new TDecisionResp(new TStatus(TStatusCode.ERROR));
      resp.getStatus().setError_message("The replicaId ( " + aReq.repid + " ) is unknown");
      return resp;
    }
    return rep.Decision(aReq.slot_num, aReq.operation);
  }
  
  @Override
  public TGetCBlockMetaResp GetCBlockMeta(TGetCBlockMetaReq req)
      throws TException {
    TGetCBlockMetaResp resp = new TGetCBlockMetaResp();
    if (_blkMgr.getCBlkMeta().version > req.version) {
      resp.setBlkmeta(_blkMgr.getCBlkMeta());
    }
    resp.setStatus(new TStatus(TStatusCode.SUCCESS));
    return resp;
  }
}
