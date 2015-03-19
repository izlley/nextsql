package org.apache.nextsql.server;

import java.security.SecureRandom;
import java.util.UUID;

public class RandomIdGenerator {
  
  private static final SecureRandom _randIdGen = new SecureRandom();
  
  public static String getNewBlockId() {
    return UUID.randomUUID().toString();
  }
  
  public static String getNewReplicaId(String aBlkId) {
    // replica id = <blockId[uuid]>-<random-int>
    return aBlkId + "-" + _randIdGen.nextInt();
  }
  
  public static long getNewOperationId() {
    return _randIdGen.nextLong();
  }
}
