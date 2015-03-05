package org.apache.nextsql.server;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.nextsql.common.NextSqlException;
import org.apache.nextsql.multipaxos.Replica;
import org.apache.nextsql.multipaxos.blockmanager.IBlockManager;
import org.apache.nextsql.multipaxos.nodemanager.INodeManager;
import org.apache.nextsql.storage.IStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockManager implements IBlockManager {
  private static final Logger LOG = LoggerFactory.getLogger(BlockManager.class);
  
  private final Map<String, PBMVal> _pathBlkMap = new HashMap<String, PBMVal>();
  private final Map<Long, Replica> _blkReplicaMap = new HashMap<Long, Replica>();
  private final BlockIdGenerator _blkIdGen;
  private final ReentrantReadWriteLock _bnLock = new ReentrantReadWriteLock(true);
  private final Lock _readLock = _bnLock.readLock();
  private final Lock _writeLock = _bnLock.writeLock();
  private final int _maxReplication;
  private final int _minReplication;
  private final INodeManager _nodeMgr;
  
  public BlockManager(int aNumReplication, NodeManager aNodeMgr)
    throws NextSqlException {
    this._blkIdGen = new BlockIdGenerator(this);
    this._maxReplication = aNumReplication;
    this._minReplication = _maxReplication / 2 + 1;
    this._nodeMgr = aNodeMgr;
  }
  
  protected class PBMVal {
    protected final Long _blkId;
    protected final List<Long> _nodeIds;
    protected final int _leaderInd;
    private PBMVal(Long aBlkId, List<Long> aNodeIds, int aLeaderInd) {
      this._blkId = aBlkId;
      this._nodeIds = aNodeIds;
      this._leaderInd = aLeaderInd;
    }
  }
  
  public void initializeReservedRSMs(List<Long> aNodeIds) throws NextSqlException {
    // The Paxos group for create/remove block op
    _blkReplicaMap.put(new Long(1L), new Replica(1L, aNodeIds, 0, null, this, _nodeMgr));
    // The Paxos group for update configuration op
    _blkReplicaMap.put(new Long(2L), new Replica(2L, aNodeIds, 0, null, this, _nodeMgr));
  }
  
  public Long getBlkIdfromPath(String aFilePath) {
    if (aFilePath == null) return null;
    PBMVal val = null;
    try {
      _readLock.lock();
      val = _pathBlkMap.get(aFilePath);
    } finally {
      _readLock.unlock();
    }
    return val._blkId;
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
      PBMVal val = _pathBlkMap.get(aFilePath);
      if (val != null) {
        replica = _blkReplicaMap.get(val._blkId);
      }
    } finally {
      _readLock.unlock();
    }
    return replica;
  }
  
  public long createABlock(String aFilePath, IStorage aStorage) throws NextSqlException {
    long blkId = _blkIdGen.nextValue();
    List<Long> nodeIds = _nodeMgr.getRandomNodeList(_maxReplication);
    int leaderIdx = 0;
    Replica newReplicas = new Replica(blkId, nodeIds, leaderIdx, aStorage, this, _nodeMgr);
    try {
      _writeLock.lock();
      if (_pathBlkMap.put(aFilePath, new PBMVal(blkId, nodeIds, leaderIdx)) != null) {
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
      PBMVal blk = _pathBlkMap.remove(aFilePath);
      if (blk == null) {
        throw new NextSqlServerException("The file(" + aFilePath + ") is unknown");
      } else {
        removeABlock(blk._blkId);
      }
    } finally {
      _writeLock.unlock();
    }
  }
  
  private void removeABlock(Long aBlkId) throws NextSqlServerException {
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
