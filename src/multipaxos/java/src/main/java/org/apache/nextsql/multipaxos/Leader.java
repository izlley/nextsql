package org.apache.nextsql.multipaxos;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.nextsql.thrift.TBallotNum;
import org.apache.nextsql.thrift.TOperation;
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
  
  public Leader(TBallotNum aInitBn, boolean aIsLeader) {
    _ballotNum = aInitBn;
    if (aIsLeader) {
      _active.set(true);
    }
  }
  
  public TBallotNum getBallotNum() {
    if (_ballotNum == null) return null;
    try {
      _readLock.lock();
      return _ballotNum;
    } finally {
      _readLock.unlock();
    }
  }
  
  public TBallotNum getBallotNumClone() {
    if (_ballotNum == null) return null;
    try {
      _readLock.lock();
      return new TBallotNum(_ballotNum);
    } finally {
      _readLock.unlock();
    }
  }
  
  public void setBallotNum(TBallotNum aBN) {
    try {
      _writeLock.lock();
      _ballotNum = aBN;
    } finally {
      _writeLock.unlock();
    }
  }
  
  public TBallotNum increaseAndGetBN() {
    if (_ballotNum == null) return null;
    try {
      _writeLock.lock();
      ++_ballotNum.id;
      return _ballotNum;
    } finally {
      _writeLock.unlock();
    }
  }
}
