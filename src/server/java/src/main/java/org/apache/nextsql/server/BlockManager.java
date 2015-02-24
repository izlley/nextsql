package org.apache.nextsql.server;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.nextsql.common.NextSqlException;
import org.apache.nextsql.multipaxos.Replica;
import org.apache.nextsql.multipaxos.thrift.TNetworkAddress;
import org.apache.nextsql.storage.IStorage;
import org.apache.nextsql.util.SequentialNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockManager {
  private static final Logger LOG = LoggerFactory.getLogger(BlockManager.class);
  
  private Map<String, Long> _PathBlkMap = new HashMap<String, Long>();
  private Map<Long, Replica> _blkReplicaMap = new HashMap<Long, Replica>();
  private final BlockIdGenerator _blkIdGen;
  private final ReentrantReadWriteLock _bnLock = new ReentrantReadWriteLock(true);
  private final Lock _readLock = _bnLock.readLock();
  private final Lock _writeLock = _bnLock.writeLock();
  private final int _maxReplication;
  private final int _minReplication;
  
  public BlockManager(int aNumReplication) {
    this._blkIdGen = new BlockIdGenerator(this);
    this._maxReplication = aNumReplication;
    this._minReplication = _maxReplication / 2 + 1;
  }
  
  public Long getBlkIDfromPath(String aFilePath) {
    if (aFilePath == null) return null;
    Long blkId = null;
    try {
      _readLock.lock();
      blkId = _PathBlkMap.get(aFilePath);
    } finally {
      _readLock.unlock();
    }
    return blkId;
  }
  
  public Replica getReplicafromBlkID(Long aBlkId) {
    if (aBlkId == null) return null;
    Replica replica = null;
    try {
      _readLock.lock();
      replica = _blkReplicaMap.get(aBlkId);
    } finally {
      _readLock.unlock();
    }
    return replica;
  }
  
  public Replica getReplicafromPath(String aFilePath) {
    if (aFilePath == null) return null;
    Replica replica = null;
    try {
      _readLock.lock();
      Long blkId = _PathBlkMap.get(aFilePath);
      if (blkId != null) {
        replica = _blkReplicaMap.get(blkId);
      }
    } finally {
      _readLock.unlock();
    }
    return replica;
  }
  
  public long createABlock(String aFilePath, List<TNetworkAddress> aLocs,
      int aLeaderInd, IStorage aStorage) throws NextSqlException {
    long blkId = _blkIdGen.nextValue();
    Replica newReplicas = new Replica(blkId, aLocs, aLeaderInd, aStorage);
    try {
      _writeLock.lock();
      if (_PathBlkMap.put(aFilePath, blkId) != null) {
        throw new NextSqlServerException("The blockID(" + blkId + ") is already exists");
      }
      _blkReplicaMap.put(blkId, newReplicas);
    } finally {
      _writeLock.unlock();
    }
    return blkId;
  }
  
  public void removeFile(String aFilePath) throws NextSqlException {
    try {
      _writeLock.lock();
      Long blkId = _PathBlkMap.remove(aFilePath);
      if (blkId == null) {
        throw new NextSqlServerException("The file(" + aFilePath + ") is unknown");
      } else {
        removeABlock(blkId);
      }
    } finally {
      _writeLock.unlock();
    }
  }
  
  private void removeABlock(Long aBlkId) throws NextSqlException {
    Replica rep = _blkReplicaMap.remove(aBlkId);
    if (rep == null)
      throw new NextSqlServerException("The blockId(" + aBlkId + ") is not exists");
  }
  
  public boolean isBlkIdExists(long aBlkId) {
    boolean res = false;
    try {
      _readLock.lock();
      res = _blkReplicaMap.containsKey(aBlkId);
    } finally {
      _readLock.unlock();
    }
    return res;
  }
}
