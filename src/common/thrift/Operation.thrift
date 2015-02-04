
namespace java org.apache.nextsql.multipaxos.thrift
namespace cpp nextsql.thrift

enum TOpType {
  OP_READ,
  OP_WRITE,
  OP_UPDATE,
  OP_GETMETA,
  OP_SETMETA
}

struct TOperation {
  1: required i64 session_id
  2: required i64 operation_handle
  3: required TOpType operation_type
  4: optional string data
}