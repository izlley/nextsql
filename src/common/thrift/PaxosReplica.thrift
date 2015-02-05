
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

service ReplicaService {
  //CreateReplica();
  //RemoveReplica();
  //RecoverReplica();
  TExecuteOperationResp ExecuteOperation(1:TExecuteOperationReq req);
}
