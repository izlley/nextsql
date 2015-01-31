package org.apache.nextsql.storage;

public interface IStorage {
  abstract public byte[] read();
  abstract public long write(byte[] data);
}
