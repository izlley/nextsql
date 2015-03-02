package org.apache.nextsql.storage;

import org.apache.nextsql.common.NextSqlException;


public class StorageManager {
  private final IStorage _storage;

  public StorageManager(String aStoragePath) throws NextSqlException {
    if (aStoragePath == null) {
      this._storage = new TestStorage();
    } else {
      this._storage = null;
    }
  }
  
  public IStorage getStorage() {
    return _storage;
  }
}
