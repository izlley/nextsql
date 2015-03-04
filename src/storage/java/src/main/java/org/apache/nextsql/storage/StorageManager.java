package org.apache.nextsql.storage;

import org.apache.commons.lang3.reflect.ConstructorUtils;
import org.apache.nextsql.common.NextSqlException;


public class StorageManager {
  private final IStorage _storage;

  // TODO: It would be better StorageManager can manager multiple-storage back ends.
  // private HashMap<storageID, IStorage> _storageMap
  
  public StorageManager(String aStorageClassName) throws NextSqlException {
    if (aStorageClassName == null) {
      // load TestStorage class
      this._storage = new TestStorage();
    } else {
      this._storage = createStorageProvider(aStorageClassName);
    }
  }
  
  /*
   * Creates a new IStorage based on the given configuration.
   */
  private static IStorage createStorageProvider(String aClassName) {
    try {
      // Re-throw any exceptions that are encountered.
      return (IStorage) ConstructorUtils.invokeConstructor(Class.forName(aClassName));
    } catch (Exception e) {
      // Re-throw as unchecked exception.
      throw new IllegalStateException(
        "Error creating StorageProvider: " + e.getMessage(), e);
    }
  }
  
  public IStorage getStorage() {
    return _storage;
  }
}
