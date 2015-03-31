package org.apache.nextsql.multipaxos;

import java.nio.charset.Charset;
import org.apache.nextsql.common.NextSqlException;
import org.apache.nextsql.thrift.TDDLparam;
import org.apache.nextsql.thrift.TExecResult;
import org.apache.nextsql.thrift.TOpType;
import org.apache.nextsql.thrift.TRWparam;
import org.apache.nextsql.thrift.TRepNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Executor {
  private static final Logger LOG = LoggerFactory.getLogger(Executor.class);
  
  private final Replica _replica;
  
  public static final long OPEN_FILE_ALREADY_EXISTS = 1L;
  public static final long EXEC_ERROR = -1L;
  public static final long EXEC_SUCCESS = 0L;
  
  public Executor(Replica aRep) {
    this._replica = aRep;
  }
  
  public void exec(long aOpH, TOpType aType, TDDLparam aDDL,
      TRWparam aRW, TExecResult aRes) throws NextSqlException {
    LOG.debug("Try to exec the operation: type = {}, opH = {}", aType, aOpH);
    long length = 0L;
    switch (aType) {
      case OP_OPEN:
        // check the file has already opened
        if (_replica._blockMgr.isFileExists(aDDL.filename)) {
          // this retval indicates the file already exists
          aRes.retval = OPEN_FILE_ALREADY_EXISTS;
          LOG.debug("The file already exists: " + aDDL.filename);
          break;
        }
        
        TRepNode replica = null;
        for (TRepNode rep : aDDL.replicas) {
          if (rep.node_id == _replica._nodeMgr.getMyNodeId()
              && !_replica._blockMgr.isRepIdExists(rep.replica_id)) {
            replica = rep;
            break;
          }
        }

        if (replica != null) {
          try {
            _replica._storage.open(aDDL.filename, null);
          } catch (NextSqlException e) {
            aRes.retval = EXEC_ERROR;
            LOG.error("Open operation failed: " + e.getMessage(), e);
            throw e;
          }
          // TODO: It would be useful to choose storage type by storage_type
          // field in TDDLparam
          _replica._blockMgr.createFile(aDDL.filename, aDDL.block_id,
              aDDL.replicas, _replica._storage);
        } else {
          _replica._blockMgr.createFileMeta(aDDL.filename, aDDL.block_id,
              aDDL.replicas);
        }
        aRes.retval = EXEC_SUCCESS;
        break;
      case OP_READ:
        byte[] readbuf = new byte[(int) aRW.size];
        try {
          length = _replica._storage.read(readbuf, aRW.offset, aRW.size);
        } catch (NextSqlException e) {
          aRes.retval = EXEC_ERROR;
          LOG.error("Read operation failed: " + e.getMessage(), e);
          throw e;
        }
        // we need to eliminate memcpys
        aRes.buffer = new String(readbuf, Charset.forName("UTF-8"));
        aRes.retval = length;
        break;
      case OP_WRITE:
        try {
          length = _replica._storage.write(
              aRW.buffer.getBytes(Charset.forName("UTF-8")), aRW.offset,
              aRW.size);
        } catch (NextSqlException e) {
          aRes.retval = EXEC_ERROR;
          LOG.error("Write operation failed: " + e.getMessage(), e);
          throw e;
        }
        aRes.retval = length;
        break;
      case OP_UPDATE:
        aRes.retval = EXEC_SUCCESS;
        break;
      case OP_DELETE:
        try {
          _replica._storage.delete(aDDL.filename);
          _replica._blockMgr.removeFile(aDDL.filename);
        } catch (NextSqlException e) {
          aRes.retval = EXEC_ERROR;
          LOG.error("Delete operation failed: " + e.getMessage(), e);
          throw e;
        }
        aRes.retval = EXEC_SUCCESS;
        break;
      case OP_GETMETA:
        aRes.retval = EXEC_SUCCESS;
        break;
      case OP_SETMETA:
        aRes.retval = EXEC_SUCCESS;
        break;
      default:
        aRes.retval = EXEC_ERROR;
        break;
    }
  }
}
