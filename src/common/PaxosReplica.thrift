
namespace java org.apache.nextsql.multipaxos.thrift
namespace cpp nextsql.thrift

include "Status.thrift"
include "Operation.thrift"

enum TReplicaProtocolVersion {
  NEXTSQL_REPLICA_PROTOCOL_V1,
  NEXTSQL_REPLICA_PROTOCOL_V2
}

//
// Propose()
//
struct TOpenOperationReq {
  1: required TOperation operation
}

struct TOpenOperationResp {
  1: required TStatus status
  2: optional string data
}

service ReplicaService {
  TOpenOperationResp OpenOperation(1:TOpenOperationReq req);
}
