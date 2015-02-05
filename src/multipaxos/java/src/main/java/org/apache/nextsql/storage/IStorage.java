package org.apache.nextsql.storage;

public interface IStorage {
  public long read(byte[] buf, long offset, long size) throws StorageException;
  public long write(byte[] buf, long offset, long size) throws StorageException;
}
