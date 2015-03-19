
namespace java org.apache.nextsql.thrift
namespace cpp nextsql.thrift

enum TOpType {
  OP_READ,
  OP_WRITE,
  OP_OPEN,
  OP_DELETE,
  OP_UPDATE,
  OP_GETMETA,
  OP_SETMETA
}

struct TRepNode {
  1: required i64 node_id
  // replica id = <blockId>-<random-int>
  2: required string replica_id
}

struct TRWparam {
  1: required string buffer
  2: required i64 offset
  3: required i64 size
}

struct TDDLparam {
  1: required string filename
  2: optional string block_id
  3: optional list<TRepNode> replicas
  4: optional i16 storage_type
}

struct TOperation {
  1: required i64 session_id
  2: required i64 operation_handle
  3: required TOpType operation_type
  4: optional TRWparam rw_param
  5: optional TDDLparam ddl_param
}


