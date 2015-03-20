package org.apache.nextsql.storage;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.nextsql.common.NextSqlException;

public class TestStorage implements IStorage {
  private static final String _outputFilePath = "./TestStorage.txt";
  private static final DateFormat dateFormat =
    new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
  private BufferedWriter _bfw = null;

  public TestStorage() throws NextSqlException {
    try {
      File file = new File(_outputFilePath);
      if (!file.exists()) {
        file.createNewFile();
      }
      FileWriter fw = new FileWriter(file.getAbsolutePath(), true);
      this._bfw = new BufferedWriter(fw);
    } catch (Exception e) {
      throw new NextSqlException("TestStorage init failure: ", e);
    }
  }
  
  @Override
  public long read(byte[] buf, long offset, long size) throws NextSqlException {
    try {
      _bfw.append(dateFormat.format(new Date()) + ": [ReadOp], offset=" + offset
        + ", size=" + size + "\n");
    } catch (IOException e) {
      throw new NextSqlException("ReadOp Failed : offset=" + offset + ", size="
        + size);
    }
    return 0;
  }

  @Override
  public long write(byte[] buf, long offset, long size) throws NextSqlException {
    try {
      _bfw.append(dateFormat.format(new Date()) + ": [WriteOp], buf=" + 
        new String(buf, Charset.forName("UTF-8")) + "offset=" + offset + ", size="
          + size + "\n");
    } catch (IOException e) {
      throw new NextSqlException("WriteOp Failed : buf=" + 
        new String(buf, Charset.forName("UTF-8")) + "offset=" + offset + ", size="
          + size);
    }
    return 0;
  }
  
  @Override
  public void open(String filepath, String mode) throws NextSqlException {
    try {
      _bfw.append(dateFormat.format(new Date()) + ": [OpenOp], " + filepath +
        " is opened.");
    } catch (IOException e) {
      throw new NextSqlException("OpenOp Failed : filepath = " + filepath);
    }
  }
  
  @Override
  public void delete(String filepath) throws NextSqlException {
    try {
      _bfw.append(dateFormat.format(new Date()) + ": [DeleteOp], " + filepath +
        " is deleted.");
    } catch (IOException e) {
      throw new NextSqlException("DeleteOp Failed : filepath = " + filepath);
    }
  }
}
