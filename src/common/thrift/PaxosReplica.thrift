
namespace java org.apache.nextsql.thrift
namespace cpp nextsql.thrift

include "Status.thrift"
include "Operation.thrift"

enum TReplicaProtocolVersion {
  NEXTSQL_REPLICA_PROTOCOL_V1,
  NEXTSQL_REPLICA_PROTOCOL_V2
}

//
// OpenFile()
//
struct TOpenFileReq {
  1: required string file_path
  2: optional string mode
}

struct TOpenFileResp {
  1: required Status.TStatus status
  2: optional i64 blockId
}

//
// RemoveFile()
//
struct TDeleteFileReq {
  1: required string file_path
  2: optional i64 blockId
}

struct TDeleteFileResp {
  1: required Status.TStatus status
}

//
// ExecuteOperation()
//
struct TExecuteOperationReq {
  1: required string file_path
  2: required Operation.TOperation operation
}

struct TExecuteOperationResp {
  1: required Status.TStatus status
  2: optional string data
}

//
// Decision()
//
struct TDecisionReq {
  1: required i64 blockId
  2: required i64 slot_num
  3: required Operation.TOperation operation
}

struct TDecisionResp {
  1: required Status.TStatus status
}

service ReplicaService {
  TOpenFileResp OpenFile(1:TOpenFileReq req);
  TDeleteFileResp DeleteFile(1:TDeleteFileReq req);
  TExecuteOperationResp ExecuteOperation(1:TExecuteOperationReq req);
  TDecisionResp Decision(1:TDecisionReq req);
}
