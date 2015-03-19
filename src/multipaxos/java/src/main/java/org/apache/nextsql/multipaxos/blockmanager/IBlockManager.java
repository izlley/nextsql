package org.apache.nextsql.multipaxos.blockmanager;

import java.util.List;

import org.apache.nextsql.common.NextSqlException;
import org.apache.nextsql.multipaxos.Replica;
import org.apache.nextsql.storage.IStorage;
import org.apache.nextsql.thrift.TLeaderRep;
import org.apache.nextsql.thrift.TRepNode;
import org.apache.nextsql.thrift.TReplicaMeta;

public interface IBlockManager {
  public List<String> getBlkIdsfromPath(String aFilePath);
  public TReplicaMeta getRepMetafromBlkId(String aBlkId);
  public List<String> getLocalRepIdfromBlkId(String aBlkId);
  public Replica getLReplicafromBlkId(String aBlkId);
  public Replica getLReplicafromPath(String aFilePath);
  public Replica getLocalReplica(String aFilePath, String aBlkId);
  public Replica getReplicafromRepId(String aRepId);
  public void addNewBlockMeta(String aBlkId, TLeaderRep aLrep, TReplicaMeta aRepMeta)
    throws NextSqlException;
  public void addNewFiletoCBlockMeta(String aFilePath, String aBlkId, String aLRepId, long aLNodeId)
    throws NextSqlException;
  public void addReplicaAndMeta(String aBlkId, TReplicaMeta aRepMeta, String aRepId,
    Replica aRep) throws NextSqlException;
  public void createFile(String aFilePath, String aBlkId, List<TRepNode> aReps, IStorage aStorage)
    throws NextSqlException;
  public void createFileMeta(String aFilePath, String aBlkId, List<TRepNode> aReps)
    throws NextSqlException;
  public void removeFile(String aFilePath)
    throws NextSqlException;
  public void removeBlock(String aBlkId)
    throws NextSqlException;
  public List<TRepNode> getRepNodes(String aBlkId)
    throws NextSqlException;
  public boolean isBlkIdExists(String aBlkId);
  public boolean isRepIdExists(String aRepId);
  public boolean isFileExists(String aFilePath);
}
