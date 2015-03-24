package org.apache.nextsql.client;

import java.security.SecureRandom;

import org.apache.nextsql.thrift.ReplicaService;
import org.apache.nextsql.thrift.TClientBlockMeta;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class NSConnection {
  protected TTransport _transport;
  protected ReplicaService.Iface _client;
  protected String _host;
  protected int _port;
  protected final int _socketTimeout;
  private boolean _isClosed = false;
  private final SecureRandom _rand;
  // client-side block-meta cache
  static protected TClientBlockMeta _blockMeta = null;
  
  public NSConnection(String aHost, int aPort, int aTimeout) {
    this._host = aHost;
    this._port = aPort;
    this._socketTimeout = aTimeout;
    this._rand = new SecureRandom();
  }
  
  public void getConnectoin() throws NSQLException {
    openTransport();
  }
  
  public NSOperation createOperation() throws NSQLException {
    return new NSOperation(this, _client, _rand.nextLong());
  }
  
  public void close() throws NSQLException {
    if (_isClosed)
      return;
    // TODO: send close request to a server to free server-side resources
    // _client.closesession...
    if (_transport != null) {
      _transport.close();
    }
    _isClosed = true;
  }
  
  private void openTransport() throws NSQLException {
    _transport = new TSocket(_host, _port, _socketTimeout);

    TProtocol protocol = new TBinaryProtocol(_transport);
    _client = new ReplicaService.Client(protocol);
    
    try {
      _transport.open();
    } catch (TTransportException e) {
      throw new NSQLException("Could not establish connection to " + _host + ":"
        + _port + e.getMessage(), e);
    }
  }
}
