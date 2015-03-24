package org.apache.nextsql.multipaxos;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.nextsql.thrift.TAcceptedValue;
import org.apache.nextsql.thrift.TBallotNum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Acceptor {
  private static final Logger LOG = LoggerFactory.getLogger(Acceptor.class);
  
  protected TBallotNum _ballotNum = null;
  private final ReentrantReadWriteLock _bnLock = new ReentrantReadWriteLock(true);
  private final Lock _readLock = _bnLock.readLock();
  private final Lock _writeLock = _bnLock.writeLock();
  private List<TAcceptedValue> _accepted = new ArrayList<TAcceptedValue>();
  
  // TODO: need gc for _accepted
  synchronized public void addAcceptVal(TAcceptedValue aVal) {
    _accepted.add(aVal);
  }
  
  public List<TAcceptedValue> getAcceptVals() {
    return _accepted;
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
}
