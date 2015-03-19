package org.apache.nextsql.server;

import org.apache.nextsql.util.SequentialNumber;

public class SerialBlockIdGenerator extends SequentialNumber {
  // The last reserved block Id
  public static final long LAST_RESERVED_BLOCK_ID = 1024L * 1024 * 1024;
  private final BlockManager _blkMgr;
  
  SerialBlockIdGenerator(BlockManager aBlkMgr) {
    super(LAST_RESERVED_BLOCK_ID);
    this._blkMgr = aBlkMgr;
  }
  
  // I decide to use UUID for blockId
  /*
  @Override
  public long nextValue() {
    long blkId = super.nextValue();
    while(!isValidBlkId(blkId)) {
      blkId = super.nextValue();
    }
    return blkId;
  }
  
  private boolean isValidBlkId(long aBlkId) {
    return (_blkMgr.isBlkIdExists(aBlkId) == false);
  }
  */
}
