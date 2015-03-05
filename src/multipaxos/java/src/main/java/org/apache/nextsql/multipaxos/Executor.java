package org.apache.nextsql.multipaxos;

import java.nio.charset.Charset;

import org.apache.nextsql.common.NextSqlException;
import org.apache.nextsql.multipaxos.blockmanager.IBlockManager;
import org.apache.nextsql.storage.IStorage;
import org.apache.nextsql.thrift.TOpType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Executor {
  private static final Logger LOG = LoggerFactory.getLogger(Executor.class);
  
  private final IStorage _storage;
  private final IBlockManager _blkMgr;
  
  public Executor(IStorage aStorage, IBlockManager aBlkMgr) {
    this._storage = aStorage;
    this._blkMgr = aBlkMgr;
  }
  
  public String exec(TOpType aType, String aData, long aOffset, long aSize, short aRetry)
      throws NextSqlException {
    LOG.debug("Try to exec the operation: type = " + aType + ", data = " + aData);
    synchronized(this) {
      for (int i = aRetry; i > 0; --i) {
        switch (aType) {
          case OP_OPEN:
            Long blkId = _blkMgr.getBlkIdfromPath(aData);
            // block is already exists
            if (blkId != null)
              return blkId.toString();
            try {
              _storage.open(aData, null);
            } catch (NextSqlException e) {
              LOG.error("Open operation failed (retry cnt:" + (aRetry - i) + "): "
                + e.getMessage());
              // if the last try is failed, throw exception
              if (i == 1) throw e;
              continue;
            }
            // create a new block
            blkId = _blkMgr.createABlock(aData, _storage);
            return blkId.toString();
          case OP_READ:
            byte[] readbuf = new byte[(int) aSize];
            try {
              _storage.read(readbuf, aOffset, aSize);
            } catch (NextSqlException e) {
              LOG.error("Read operation failed (retry cnt:" + (aRetry - i) + "): "
                + e.getMessage());
              // if the last try is failed, throw exception
              if (i == 1) throw e;
              continue;
            }
            // we need to eliminate memcpys
            return new String(readbuf, Charset.forName("UTF-8"));
          case OP_WRITE:
            long length = 0;
            try {
              length = _storage.write(aData.getBytes(Charset.forName("UTF-8")),
                aOffset, aSize);
            } catch (NextSqlException e) {
              LOG.error("Write operation failed (retry cnt:" + (aRetry - i) + "): "
                + e.getMessage());
              // if the last try is failed, throw exception
              if (i == 1) throw e;
              continue;
            }
            return String.valueOf(length);
          case OP_UPDATE:
            break;
          case OP_DELETE:
            try {
              _blkMgr.removeFile(aData);
              _storage.delete(aData);
            } catch (NextSqlException e) {
              LOG.error("Delete operation failed (retry cnt:" + (aRetry - i) + "): "
                + e.getMessage());
              // if the last try is failed, throw exception
              if (i == 1) throw e;
              continue;
            }
            return null;
          case OP_GETMETA:
            break;
          case OP_SETMETA:
            break;
          default:
            break;
        }
      }
    }
    return null;
  }
}
