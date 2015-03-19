
namespace java org.apache.nextsql.thrift
namespace cpp nextsql.thrift

include "Status.thrift"
include "Operation.thrift"
include "BlockMeta.thrift"

enum TReplicaProtocolVersion {
  NEXTSQL_REPLICA_PROTOCOL_V1,
  NEXTSQL_REPLICA_PROTOCOL_V2
}

//
// OpenFile()
//
struct TOpenFileReq {
  1: required string file_path
  2: required i64 blkmeta_version
  3: optional string mode
}

struct TOpenFileResp {
  1: required Status.TStatus status
  2: required string blkId
  3: optional BlockMeta.TClientBlockMeta blkmeta
}

//
// DeleteFile()
//
struct TDeleteFileReq {
  1: required string file_path
  2: required i64 blkmeta_version
  3: optional string blockId
}

struct TDeleteFileResp {
  1: required Status.TStatus status
  2: optional BlockMeta.TClientBlockMeta blkmeta
}

//
// ExecuteOperation()
//
struct TExecuteOperationReq {
  1: required string blkId
  2: required Operation.TOperation operation
  3: required i64 blkmeta_version
  4: optional string file_path
}

struct TExecResult {
  1: required i64 retval
  2: optional string buffer
}

struct TExecuteOperationResp {
  1: required Status.TStatus status
  2: optional TExecResult result
  3: optional BlockMeta.TClientBlockMeta blkmeta
}

//
// Decision()
//
struct TDecisionReq {
  1: required string repid
  2: required i64 slot_num
  3: required Operation.TOperation operation
}

struct TDecisionResp {
  1: required Status.TStatus status
  2: optional TExecResult result
}

//
// GetCBlockMeta()
//
struct TGetCBlockMetaReq {
  1: required i64 version
}

struct TGetCBlockMetaResp {
  1: required Status.TStatus status
  2: optional BlockMeta.TClientBlockMeta blkmeta
}

service ReplicaService {
  TOpenFileResp OpenFile(1:TOpenFileReq req);
  TDeleteFileResp DeleteFile(1:TDeleteFileReq req);
  TExecuteOperationResp ExecuteOperation(1:TExecuteOperationReq req);
  TDecisionResp Decision(1:TDecisionReq req);
  TGetCBlockMetaResp GetCBlockMeta(1:TGetCBlockMetaReq req);
}
