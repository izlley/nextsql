package org.apache.nextsql.multipaxos;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.nextsql.multipaxos.thrift.TBallotNum;
import org.apache.nextsql.multipaxos.thrift.TNetworkAddress;
import org.apache.nextsql.multipaxos.thrift.TOperation;
import org.apache.nextsql.util.SystemInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Leader {
  private static final Logger LOG = LoggerFactory.getLogger(Leader.class);
  protected AtomicBoolean _active = new AtomicBoolean(false);
  protected TBallotNum _ballotNum = null;
  protected Map<Long, TOperation> _proposals = new HashMap<Long, TOperation>();
  
  public Leader() throws MultiPaxosException {
    TNetworkAddress addr = SystemInfo.getNetworkAddress();
    if (addr == null) {
      throw new MultiPaxosException("getNetworkAddress failed");
    }
    _ballotNum = new TBallotNum(0, addr);
  }
}
