package org.apache.nextsql.multipaxos;

public class Operation {
  long _sessionid;
  short _opid;
  String _op;
  public Operation (long sessid, short opid, String opdata) {
    this._sessionid = sessid;
    this._opid = opid;
    this._op = opdata;
  }
}
