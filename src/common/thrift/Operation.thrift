
namespace java org.apache.nextsql.multipaxos.thrift
namespace cpp nextsql.thrift

enum TOpType {
  OP_READ,
  OP_WRITE,
  OP_CREATE,
  OP_REMOVE,
  OP_UPDATE,
  OP_GETMETA,
  OP_SETMETA
}

struct TOperation {
  1: required i64 session_id
  2: required i64 operation_handle
  3: required TOpType operation_type
  4: required i64 block_id
  5: optional string data
  6: optional i64 offset
  7: optional i64 size
}
