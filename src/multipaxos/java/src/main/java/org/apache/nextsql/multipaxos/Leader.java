package org.apache.nextsql.multipaxos;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.nextsql.multipaxos.nodemanager.INodeManager;
import org.apache.nextsql.multipaxos.thrift.TBallotNum;
import org.apache.nextsql.multipaxos.thrift.TNetworkAddress;
import org.apache.nextsql.multipaxos.thrift.TOperation;
import org.apache.nextsql.multipaxos.util.SystemInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Leader {
  private static final Logger LOG = LoggerFactory.getLogger(Leader.class);
  protected AtomicBoolean _active = new AtomicBoolean(false);
  protected TBallotNum _ballotNum = null;
  private final ReentrantReadWriteLock _bnLock = new ReentrantReadWriteLock(true);
  private final Lock _readLock = _bnLock.readLock();
  private final Lock _writeLock = _bnLock.writeLock();
  protected ConcurrentHashMap<Long, TOperation> _proposals = new ConcurrentHashMap<Long, TOperation>();
  
  public Leader(INodeManager aNodeMgr) throws MultiPaxosException {
    TNetworkAddress addr = SystemInfo.getNetworkAddress();
    if (addr == null) {
      throw new MultiPaxosException("getNetworkAddress failed");
    }
    _ballotNum = new TBallotNum(0L, aNodeMgr.getNodeId(addr));
  }
  
  public TBallotNum getBallotNum() {
    _readLock.lock();
    TBallotNum bn = _ballotNum;
    _readLock.unlock();
    return bn;
  }
  
  public void setBallotNum(TBallotNum aBN) {
    _writeLock.lock();
    _ballotNum = aBN;
    _writeLock.unlock();
  }
  
  public TBallotNum increaseAndGetBN() {
    _writeLock.lock();
    ++_ballotNum.id;
    TBallotNum bn = _ballotNum;
    _writeLock.unlock();
    return bn;
  }
}
