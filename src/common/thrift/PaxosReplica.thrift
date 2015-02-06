
namespace java org.apache.nextsql.multipaxos.thrift
namespace cpp nextsql.thrift

include "Status.thrift"
include "Operation.thrift"

enum TReplicaProtocolVersion {
  NEXTSQL_REPLICA_PROTOCOL_V1,
  NEXTSQL_REPLICA_PROTOCOL_V2
}

//
// ExecuteOperation()
//
struct TExecuteOperationReq {
  1: required Operation.TOperation operation
}

struct TExecuteOperationResp {
  1: required Status.TStatus status
  2: optional string data
}

//
// Decision()
//
struct TDecisionReq {
  1: required i64 slot_num
  2: required Operation.TOperation operation
}

struct TDecisionResp {
  1: required Status.TStatus status
}

service ReplicaService {
  //CreateReplica();
  //RemoveReplica();
  //RecoverReplica();
  TExecuteOperationResp ExecuteOperation(1:TExecuteOperationReq req);
  TDecisionResp Decision(1:TDecisionReq req);
}
