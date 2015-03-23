package org.apache.nextsql.server;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.nextsql.common.NextSqlException;
import org.apache.nextsql.multipaxos.nodemanager.INodeManager;
import org.apache.nextsql.multipaxos.util.SystemInfo;
import org.apache.nextsql.thrift.TNetworkAddress;
import org.apache.nextsql.thrift.TNodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeManager implements INodeManager {
  private static final Logger LOG = LoggerFactory.getLogger(NodeManager.class);
  
  private final TNodeInfo _nodeInfo;
  private final NodeIdGenerator _nodeIdGen;
  private long _myNodeId = -1;
  private final ReentrantReadWriteLock _bnLock = new ReentrantReadWriteLock(true);
  private final Lock _readLock = _bnLock.readLock();
  private final Lock _writeLock = _bnLock.writeLock();
  private final Random _rand = new Random();
  // TODO: We need heartbeat process to check other node's liveness
  
  private volatile boolean isInitialized = false;
  
  public NodeManager() throws NextSqlException {
    this._nodeInfo = new TNodeInfo(new HashMap<Long, TNetworkAddress>(), 0L);
    this._nodeIdGen = new NodeIdGenerator(this);
    initialize();
  }
  
  public void initialize() throws NextSqlException {
    String[] nodeList =
      NextSqlServer._conf.getStrings(NextSqlConfigKeys.NS_NODE_HOSTNAME_LIST,
        NextSqlConfigKeys.NS_NODE_ADDR_LIST_DEFAULT);
    long nid;
    for (int i = 0; i < nodeList.length; i++) {
      String ip;
      try {
        ip = InetAddress.getByName(nodeList[i]).getHostAddress();
      } catch (UnknownHostException e) {
        LOG.error("Unknown hostname : " + nodeList[i]);
        throw new NextSqlException("Unknown hostname : " + nodeList[i]);
      }
      nid = addNode(new TNetworkAddress(ip,
          NextSqlServer._conf.getInt(NextSqlConfigKeys.NS_REPLICA_SERVER_PORT,
            NextSqlConfigKeys.NS_REPLICA_SERVER_PORT_DEFAULT),
          NextSqlServer._conf.getInt(NextSqlConfigKeys.NS_PAXOS_SERVER_PORT,
            NextSqlConfigKeys.NS_PAXOS_SERVER_PORT_DEFAULT))
        );
      if (SystemInfo.getNetworkAddress().getHostname().equals(nodeList[i])) {
        _myNodeId = nid;
      }
    }
    if (_myNodeId == -1) {
      throw new NextSqlException("NextSql nodelist configuration doesn't include current node.");
    }
    isInitialized = true;
  }
  
  public TNetworkAddress getNodeInfo(Long aNodeId) {
    if (aNodeId == null) return null;
    TNetworkAddress addr = null;
    try {
      _readLock.lock();
      addr = _nodeInfo.nodeidinfo_map.get(aNodeId);
    } finally {
      _readLock.unlock();
    }
    return addr;
  }
  
  // TODO: Do we need bidimap for performance?
  public Long getNodeId(TNetworkAddress aAddr) {
    try {
      _readLock.lock();
      for (Entry<Long, TNetworkAddress> en: _nodeInfo.nodeidinfo_map.entrySet()) {
        if (aAddr.equals(en.getValue())) {
          return en.getKey();
        }
      }
      return null;
    } finally {
      _readLock.unlock();
    }
  }
  
  public long addNode(TNetworkAddress aAddr) throws NextSqlException {
    long nid = _nodeIdGen.nextValue();
    try {
      _writeLock.lock();
      if (_nodeInfo.nodeidinfo_map.containsKey(nid)) {
        throw new NextSqlException("The NodeID(" + nid + ") is already exists"); 
      }
      _nodeInfo.nodeidinfo_map.put(nid, aAddr);
      ++_nodeInfo.version;
    } finally {
      _writeLock.unlock();
    }
    return nid;
  }
  
  public void removeNode(Long aNodeId) {
    try {
      _writeLock.lock();
      _nodeInfo.nodeidinfo_map.remove(aNodeId);
      ++_nodeInfo.version;
    } finally {
      _writeLock.unlock();
    }
  }
  
  public boolean isNodeIdExists(long aNId) {
    return _nodeInfo.nodeidinfo_map.containsKey(aNId);
  }
  
  public List<Long> getRandomNodeList(int aSize) throws NextSqlException {
    if (aSize > _nodeInfo.nodeidinfo_map.size())
      throw new NextSqlException("Number of nodes is smaller than the replication factor.");
    List<Long> nodelist = new ArrayList<Long>(aSize);
    try {
      Field table = HashMap.class.getDeclaredField("table");
      table.setAccessible(true);
      _readLock.lock();
      Entry<Long, TNetworkAddress>[] entries =
        (Entry<Long, TNetworkAddress>[]) table.get(_nodeInfo.nodeidinfo_map);
      for (int i = 0; i < aSize; i++) {
        int start = _rand.nextInt(entries.length);
        for (int j = 0; j < entries.length; j++) {
          int idx = (start + j) % entries.length;
          Entry<Long, TNetworkAddress> entry = entries[idx];
          if (entry != null && !nodelist.contains(entry.getKey())) {
            nodelist.add(entry.getKey());
            break;
          }
        }
      }
    } catch (Exception e) {
      throw new NextSqlException(e.getMessage());
    } finally {
      _readLock.unlock();
    }
    return nodelist;
  }
  
  public List<Long> getAllNodeIds() {
    try {
      _readLock.lock();
      return new ArrayList<Long>(_nodeInfo.nodeidinfo_map.keySet());
    } finally {
      _readLock.unlock();
    }
  }
  
  public long getMyNodeId() {
    return this._myNodeId;
  }
}
