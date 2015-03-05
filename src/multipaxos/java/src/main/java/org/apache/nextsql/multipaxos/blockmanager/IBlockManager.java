package org.apache.nextsql.multipaxos.blockmanager;

import org.apache.nextsql.common.NextSqlException;
import org.apache.nextsql.multipaxos.Replica;
import org.apache.nextsql.storage.IStorage;

public interface IBlockManager {
  public Long getBlkIdfromPath(String aFilePath);
  public Replica getReplicafromBlkID(Long aBlkId);
  public Replica getReplicafromPath(String aFilePath);
  public long createABlock(String aFilePath, IStorage aStorage) throws NextSqlException;
  public void removeFile(String aFilePath) throws NextSqlException;
}
