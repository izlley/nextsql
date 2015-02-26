package org.apache.nextsql.storage;

import java.io.File;

public interface IStorage {
  public long read(byte[] buf, long offset, long size) throws StorageException;
  public long write(byte[] buf, long offset, long size) throws StorageException;
  public boolean open(String filepath, String mode) throws StorageException;
  public boolean delete(String filepath) throws StorageException;
}
