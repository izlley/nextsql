package org.apache.nextsql.multipaxos;

import java.util.ArrayList;
import java.util.List;

import org.apache.nextsql.multipaxos.thrift.TAcceptedValue;
import org.apache.nextsql.multipaxos.thrift.TBallotNum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Acceptor {
  private static final Logger LOG = LoggerFactory.getLogger(Acceptor.class);
  
  protected TBallotNum _ballotNum = null;
  private List<TAcceptedValue> _accepted = new ArrayList<TAcceptedValue>();
  
  // TODO: need gc for _accepted
  
  public void addAcceptVal(TAcceptedValue aVal) {
    _accepted.add(aVal);
  }
  
  public List<TAcceptedValue> getAcceptVals() {
    return _accepted;
  }
}
