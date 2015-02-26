package org.apache.nextsql.storage;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TestStorage implements IStorage {
  private static final String _outputFilePath = "./TestStorage.txt";
  private static final DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
  private BufferedWriter _bfw = null;

  public TestStorage() throws Exception {
    File file = new File(_outputFilePath);
    if (!file.exists()) {
      file.createNewFile();
    }
    FileWriter fw = new FileWriter(file.getAbsolutePath(), true);
    this._bfw = new BufferedWriter(fw);
  }
  
  @Override
  public long read(byte[] buf, long offset, long size) throws StorageException {
    try {
      _bfw.append(dateFormat.format(new Date()) + ": [ReadOp], offset=" + offset + ", size=" + size + "\n");
    } catch (IOException e) {
      throw new StorageException("ReadOp Failed : offset=" + offset + ", size=" + size);
    }
    return 0;
  }

  @Override
  public long write(byte[] buf, long offset, long size) throws StorageException {
    try {
      _bfw.append(dateFormat.format(new Date()) + ": [WriteOp], buf=" + 
        new String(buf, Charset.forName("UTF-8")) + "offset=" + offset + ", size=" + size + "\n");
    } catch (IOException e) {
      throw new StorageException("WriteOp Failed : buf=" + 
        new String(buf, Charset.forName("UTF-8")) + "offset=" + offset + ", size=" + size);
    }
    return 0;
  }
  
  @Override
  public boolean open(String filepath, String mode) throws StorageException {
    try {
      _bfw.append(dateFormat.format(new Date()) + ": [OpenOp], " + filepath + " is opened.");
    } catch (IOException e) {
      throw new StorageException("OpenOp Failed : filepath = " + filepath);
    }
    return true;
  }
  
  @Override
  public boolean delete(String filepath) throws StorageException {
    try {
      _bfw.append(dateFormat.format(new Date()) + ": [DeleteOp], " + filepath + " is deleted.");
    } catch (IOException e) {
      throw new StorageException("DeleteOp Failed : filepath = " + filepath);
    }
    return true;
  }
}
