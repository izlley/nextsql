package org.apache.nextsql.client;

import org.apache.nextsql.thrift.TStatus;
import org.apache.nextsql.thrift.TStatusCode;

public class Utils {
  public static void verifySuccess(TStatus status) throws NSQLException {
    if (status.getStatus_code() != TStatusCode.SUCCESS) {
      throw new NSQLException(status.getError_message());
      }
  }
}
