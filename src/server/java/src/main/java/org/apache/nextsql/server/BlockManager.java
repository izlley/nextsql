package org.apache.nextsql.server;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.nextsql.common.NextSqlException;
import org.apache.nextsql.multipaxos.Replica;
import org.apache.nextsql.multipaxos.blockmanager.IBlockManager;
import org.apache.nextsql.multipaxos.nodemanager.INodeManager;
import org.apache.nextsql.storage.IStorage;
import org.apache.nextsql.thrift.TBlkIdRepMeta;
import org.apache.nextsql.thrift.TClientBlockMeta;
import org.apache.nextsql.thrift.TLeaderRep;
import org.apache.nextsql.thrift.TNetworkAddress;
import org.apache.nextsql.thrift.TRepNode;
import org.apache.nextsql.thrift.TReplicaMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockManager implements IBlockManager {
  private static final Logger LOG = LoggerFactory.getLogger(BlockManager.class);
  
  private final TClientBlockMeta _cblkMeta;
  private final TBlkIdRepMeta _blkIdRepMeta;
  private final Map<String, Replica> _repIdReplicaMap = new HashMap<String, Replica>();
  private final ReentrantReadWriteLock _bnLock = new ReentrantReadWriteLock(true);
  private final Lock _readLock = _bnLock.readLock();
  private final Lock _writeLock = _bnLock.writeLock();
  private final int _maxReplication;
  private final int _minReplication;
  private final INodeManager _nodeMgr;
  private final IStorage _storageMgr;
  
  public BlockManager(int aNumReplication, NodeManager aNodeMgr, IStorage aStorage)
    throws NextSqlException {
    this._cblkMeta = new TClientBlockMeta(
        new HashMap<String, List<String>>(),
        new HashMap<String, TLeaderRep>(), 0L);
    this._blkIdRepMeta = new TBlkIdRepMeta(new HashMap<String, TReplicaMeta>(), 0L);
    this._maxReplication = aNumReplication;
    this._minReplication = _maxReplication / 2 + 1;
    this._nodeMgr = aNodeMgr;
    this._storageMgr = aStorage;
  }
  
  public List<String> getBlkIdsfromPath(String aFilePath) {
    if (aFilePath == null) return null;
    List<String> val = null;
    try {
      _readLock.lock();
      // O(1)
      val = _cblkMeta.fileblkid_map.get(aFilePath);
    } finally {
      _readLock.unlock();
    }
    return val;
  }
  
  @Override
  public TReplicaMeta getRepMetafromBlkId(String aBlkId) {
    if (aBlkId == null) return null;
    TReplicaMeta repMeta = null;
    try {
      _readLock.lock();
      // O(1)
      repMeta = _blkIdRepMeta.blkidrepmeta_map.get(aBlkId);
    } finally {
      _readLock.unlock();
    }
    return repMeta;
  }
  
  @Override
  public List<String> getLocalRepIdfromBlkId(String aBlkId) {
    TReplicaMeta repMeta = getRepMetafromBlkId(aBlkId);
    List<String> repIds = new LinkedList<String>();
    if (repMeta != null) {
      for (TRepNode rep: repMeta.replicas) {
        if (rep.node_id == _nodeMgr.getMyNodeId()) {
          repIds.add(rep.replica_id);
        }
      }
    }
    return repIds;
  }
  
  @Override
  public Replica getLReplicafromBlkId(String aBlkId) {
    if (aBlkId == null) return null;
    TLeaderRep repinfo = null;
    Replica replica = null;
    try {
      _readLock.lock();
      // O(1)
      repinfo = _cblkMeta.blkidleader_map.get(aBlkId);
      if (repinfo != null) {
        // O(1)
        replica = _repIdReplicaMap.get(repinfo.leader_repid);
      }
    } finally {
      _readLock.unlock();
    }
    return replica;
  }
  
  @Override
  public Replica getLReplicafromPath(String aFilePath) {
    if (aFilePath == null) return null;
    TLeaderRep repinfo = null;
    Replica replica = null;
    try {
      _readLock.lock();
      // O(1)
      List<String> blkIds = _cblkMeta.fileblkid_map.get(aFilePath);
      if (blkIds != null) {
        // return the first block's leader replica
        // O(1)
        repinfo = _cblkMeta.blkidleader_map.get(blkIds.get(0));
        if (repinfo != null) {
          // O(1)
          replica = _repIdReplicaMap.get(repinfo.leader_repid);
        }
      }
    } finally {
      _readLock.unlock();
    }
    return replica;
  }
  
  private Replica getLocalReplicafromBlkId(String aBlkId) {
    TReplicaMeta repMeta = null;
    Replica replica = null;
    // return the first block's repMeta
    // O(1)
    repMeta = _blkIdRepMeta.blkidrepmeta_map.get(aBlkId);
    if (repMeta != null) {
      // first check current node has leader replica
      TRepNode repNode = repMeta.replicas.get(repMeta.leader_idx);
      if (repNode.node_id == _nodeMgr.getMyNodeId()) {
        replica = _repIdReplicaMap.get(repNode.replica_id);
      } else {
        repNode = null;
        for (Iterator<TRepNode> it = repMeta.replicas.iterator(); it.hasNext();) {
          TRepNode curr = it.next();
          if (curr.node_id == _nodeMgr.getMyNodeId()) {
            repNode = curr;
            break;
          }
        }
        if (repNode != null) {
          replica = _repIdReplicaMap.get(repNode.replica_id);
        }
      }
    }
    return replica;
  }
  
  @Override
  public Replica getLocalReplica(String aFilePath, String aBlkId) {
    if (aFilePath == null && aBlkId == null) return null;
    Replica replica = null;
    try {
      _readLock.lock();
      if (aFilePath != null) {
        // O(1)
        List<String> blkIds = _cblkMeta.fileblkid_map.get(aFilePath);
        if (blkIds != null) {
          replica = getLocalReplicafromBlkId(blkIds.get(0));
        }
      } else if (aBlkId != null) {
        replica = getLocalReplicafromBlkId(aBlkId);
      }
    } finally {
      _readLock.unlock();
    }
    return replica;
  }
  
  @Override
  public Replica getReplicafromRepId(String aRepId) {
    if (aRepId == null) return null;
    Replica replica = null;
    try {
      _readLock.lock();
      // O(1)
      replica = _repIdReplicaMap.get(aRepId);
    } finally {
      _readLock.unlock();
    }
    return replica;
  }
  
  public void initializeReservedRSMs() throws NextSqlException {
    // The Paxos group for create/remove block op
    createReservedRSM("DDL", _nodeMgr.getAllNodeIds());
    // The Paxos group for update configuration op
    createReservedRSM("CONF", _nodeMgr.getAllNodeIds());
  }
  
  private void createReservedRSM(String aBlkId, List<Long> aNodes)
      throws NextSqlException {
    final int leaderIdx = 0;
    String localRepId = null;
    List<TRepNode> replicas = new LinkedList<TRepNode>();
    for (long nid: aNodes) {
      String repid = aBlkId + "-" + nid;
      replicas.add(new TRepNode(nid, repid));
      if (nid == _nodeMgr.getMyNodeId()) {
        localRepId = repid;
      }
    }
    if (localRepId == null) {
      throw new NextSqlServerException("Reserved local replca id is null.");
    }
    TLeaderRep leaderRep = new TLeaderRep(replicas.get(leaderIdx).replica_id,
      _nodeMgr.getNodeInfo(replicas.get(leaderIdx).node_id));
    TReplicaMeta repMeta = new TReplicaMeta(replicas, leaderIdx);
    addNewBlockMeta(aBlkId, leaderRep, repMeta);
    try {
      _writeLock.lock();
      if (_repIdReplicaMap.containsKey(localRepId)) {
        throw new NextSqlServerException("The replcaId (" + localRepId + ") already exists.");
      }
      _repIdReplicaMap.put(localRepId, new Replica(aBlkId, localRepId, replicas, leaderIdx,
        _storageMgr, this, _nodeMgr, (_nodeMgr.getMyNodeId() == replicas.get(leaderIdx).node_id)));
    } finally {
      _writeLock.unlock();
    }
  }
  
  @Override
  public void addNewBlockMeta(String aBlkId, TLeaderRep aLrep, TReplicaMeta aRepMeta)
      throws NextSqlException {
    try {
      _writeLock.lock();
      if (_cblkMeta.blkidleader_map.containsKey(aBlkId) ||
          _blkIdRepMeta.blkidrepmeta_map.containsKey(aBlkId)) {
        throw new NextSqlServerException("BlockId (" + aBlkId + ") is already exists.");
      }
      _cblkMeta.blkidleader_map.put(aBlkId, aLrep);
      ++_cblkMeta.version;
      _blkIdRepMeta.blkidrepmeta_map.put(aBlkId, aRepMeta);
      ++_blkIdRepMeta.version;
    } finally {
      _writeLock.unlock();
    }
  }
  
  @Override
  public void addNewFiletoCBlockMeta(String aFilePath, String aBlkId, String aLRepId, long aLNodeId)
      throws NextSqlException {
    if (aFilePath == null || aLRepId == null) {
      throw new NextSqlServerException("Invalid filepath or replica Id. (null)");
    }
    List<String> blkIds = new LinkedList<String>();
    blkIds.add(aBlkId);
    TNetworkAddress leaderAddr = _nodeMgr.getNodeInfo(aLNodeId);
    if (leaderAddr == null) {
      throw new NextSqlServerException("The leader nodeId ( " + aLNodeId + " ) does not exists.");
    }
    TLeaderRep Lrep = new TLeaderRep(aLRepId, leaderAddr);
    // O(1)
    if (_cblkMeta.fileblkid_map.containsKey(aFilePath)) {
      throw new NextSqlServerException("The file ( " + aFilePath
        + " ) already exists.");
    }
    // O(1)
    if (_cblkMeta.blkidleader_map.containsKey(aBlkId)) {
      throw new NextSqlServerException("The block ( " + aBlkId
        + " ) already exists.");
    }
    _cblkMeta.fileblkid_map.put(aFilePath, blkIds);
    _cblkMeta.blkidleader_map.put(aBlkId, Lrep);
    ++_cblkMeta.version;
  }
  
  @Override
  public void addReplicaAndMeta(String aBlkId, TReplicaMeta aRepMeta, String aRepId,
      Replica aRep) throws NextSqlException {
    if (_blkIdRepMeta.blkidrepmeta_map.containsKey(aBlkId)) {
      throw new NextSqlServerException("The block ( " + aBlkId
          + " ) already exists.");
    }
    if (_repIdReplicaMap.containsKey(aRepId)) {
      throw new NextSqlServerException("The replicaId ( " + aRepId
          + " ) already exists.");
    }
    _blkIdRepMeta.blkidrepmeta_map.put(aBlkId, aRepMeta);
    ++_blkIdRepMeta.version;
    _repIdReplicaMap.put(aRepId, aRep);
  }
  
  @Override
  public void createFileMeta(String aFilePath, String aBlkId, List<TRepNode> aReps)
      throws NextSqlException {
    final int leaderIdx = 0;
    if (isFileExists(aFilePath)) {
      LOG.warn("The filemeta (" + aFilePath + ") already exists.");
      return;
    }
    TReplicaMeta repMeta = new TReplicaMeta(aReps, leaderIdx);
    try {
      _writeLock.lock();
      addNewFiletoCBlockMeta(aFilePath, aBlkId, aReps.get(leaderIdx).replica_id,
        aReps.get(leaderIdx).node_id);
      _blkIdRepMeta.blkidrepmeta_map.put(aBlkId, repMeta);
      ++_blkIdRepMeta.version;
    } finally {
      _writeLock.unlock();
    }
  }
  
  @Override
  public void createFile(String aFilePath, String aBlkId, List<TRepNode> aReps, IStorage aStorage)
      throws NextSqlException {
    final int leaderIdx = 0;
    TReplicaMeta repMeta = new TReplicaMeta(aReps, leaderIdx);
    String localRepId = null;
    for (TRepNode rep: aReps) {
      if (rep.node_id == _nodeMgr.getMyNodeId() && !isRepIdExists(rep.replica_id)) {
        localRepId = rep.replica_id;
        break;
      }
    }
    if (localRepId == null) {
      throw new NextSqlServerException("The Paxos group does not include current node" +
        " or the replica already exists");
    }
    try {
      _writeLock.lock();
      addNewFiletoCBlockMeta(aFilePath, aBlkId, aReps.get(leaderIdx).replica_id,
        aReps.get(leaderIdx).node_id);
      addReplicaAndMeta(aBlkId, repMeta, localRepId,
        new Replica(aBlkId, localRepId, aReps, leaderIdx, aStorage, this, _nodeMgr,
                    (_nodeMgr.getMyNodeId() == aReps.get(leaderIdx).node_id))
      );
    } finally {
      _writeLock.unlock();
    }
  }
  
  @Override
  public void removeFile(String aFilePath) throws NextSqlException {
    try {
      _writeLock.lock();
      List<String> blocks = _cblkMeta.fileblkid_map.remove(aFilePath);
      if (blocks == null) {
        throw new NextSqlServerException("The file (" + aFilePath + ") does not exist.");
      }
      for (String blkId: blocks) {
        _cblkMeta.blkidleader_map.remove(blkId);
        ++_cblkMeta.version;
        TReplicaMeta repMeta = _blkIdRepMeta.blkidrepmeta_map.remove(blkId);
        if (repMeta != null) {
          ++_blkIdRepMeta.version;
          for (TRepNode replica : repMeta.replicas) {
            if (replica.node_id == _nodeMgr.getMyNodeId()) {
              _repIdReplicaMap.remove(replica.node_id);
            }
          }
        }
      }
    } finally {
      _writeLock.unlock();
    }
  }
  
  @Override
  public void removeBlock(String aBlkId) throws NextSqlServerException {
    try {
      _writeLock.lock();
      if (_cblkMeta.blkidleader_map.remove(aBlkId) == null) {
        throw new NextSqlServerException("The blockId (" + aBlkId + ") does not exist.");
      }
      ++_cblkMeta.version;
      TReplicaMeta repMeta = _blkIdRepMeta.blkidrepmeta_map.remove(aBlkId);
      if (repMeta != null) {
        ++_blkIdRepMeta.version;
        for (TRepNode replica : repMeta.replicas) {
          if (replica.node_id == _nodeMgr.getMyNodeId()) {
            _repIdReplicaMap.remove(replica.node_id);
          }
        }
      }
    } finally {
      _writeLock.unlock();
    }
  }
  
  @Override
  public List<TRepNode> getRepNodes(String aBlkId) throws NextSqlException {
    List<Long> nodeIds = _nodeMgr.getRandomNodeList(_maxReplication);
    List<TRepNode> repNodes = new LinkedList<TRepNode>();
    for (long nodeId: nodeIds) {
      repNodes.add(new TRepNode(nodeId, RandomIdGenerator.getNewReplicaId(aBlkId)));
    }
    return repNodes;
  }
  
  @Override
  public boolean isBlkIdExists(String aBlkId) {
    boolean res = false;
    try {
      _readLock.lock();
      res = _blkIdRepMeta.blkidrepmeta_map.containsKey(aBlkId);
    } finally {
      _readLock.unlock();
    }
    return res;
  }
  
  @Override
  public boolean isRepIdExists(String aRepId) {
    boolean res = false;
    try {
      _readLock.lock();
      res = _repIdReplicaMap.containsKey(aRepId);
    } finally {
      _readLock.unlock();
    }
    return res;
  }
  
  @Override
  public boolean isFileExists(String aFilePath) {
    boolean res = false;
    try {
      _readLock.lock();
      res = _cblkMeta.fileblkid_map.containsKey(aFilePath);
    } finally {
      _readLock.unlock();
    }
    return res;
  }
  
  public TClientBlockMeta getCBlkMeta() {
    return _cblkMeta;
  }
}
