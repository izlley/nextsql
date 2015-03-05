package org.apache.nextsql.storage;

import org.apache.nextsql.common.NextSqlException;

public interface IStorage {
  public long read(byte[] buf, long offset, long size) throws NextSqlException;
  public long write(byte[] buf, long offset, long size) throws NextSqlException;
  public void open(String filepath, String mode) throws NextSqlException;
  public void delete(String filepath) throws NextSqlException;
}
