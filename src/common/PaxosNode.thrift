
namespace java org.apache.nextsql.multipaxos.thrift
namespace cpp nextsql.thrift

include "Status.thrift"
include "Operation.thrift"

enum TPaxosProtocolVersion {
  NEXTSQL_REPLICA_PROTOCOL_V1,
  NEXTSQL_REPLICA_PROTOCOL_V2
}

struct TNetworkAddress {
  1: required string hostname
  2: required i32 port
}

struct BallotNum {
  1: required i64 id
  2: required TNetworkAddress proposer
}

struct AcceptedValue {
  1: required BallotNum ballot_num
  2: required i64 slot_num
  3: required TOperation operation
}

//
// LeaderPropose()
//
struct LeaderProposeReq {
  1: required BallotNum ballot_num
}

struct LeaderProposeResp {
  1: required TStatus status
  2: required BallotNum ballot_num
}

//
// AcceptorPhaseOne()
//
struct AcceptorPhaseOneReq {
  1: required BallotNum ballot_num
}

struct AcceptorPhaseOneResp {
  1: required BallotNum ballot_num
  2: required TNetworkAddress acceptor
  3: list<AcceptedValue> accepted_values
}

//
// LeaderAccept()
//
struct LeaderAcceptReq {
  1: required i64 slot_num
  2: required TOperation operation
}

struct LeaderAcceptResp {
  1: required TStatus status
}

//
// AcceptorPhaseTwo()
//
struct AcceptorPhaseTwoReq {
  1: required BallotNum ballot_num
  2: required i64 slot_num
  3: required TOperation operation
}

struct AcceptorPhaseTwoResp {
  1: required BallotNum ballot_num
  2: required TNetworkAddress acceptor

}

//
// Heartbeat()
//
struct HeartbeatResp {
  1: required TStatus status
}

service PaxosService {
  LeaderProposeResp LeaderPropose(1:LeaderProposeReq req);
  
  LeaderAcceptResp LeaderAccept(1:LeaderAcceptReq req);
  
  AcceptorPhaseOneResp AcceptorPhaseOne(1:AcceptorPhaseOneReq req);
  
  AcceptorPhaseTwoResp AcceptorPhaseTwo(1:AcceptorPhaseTwoReq req);
  
  HeartbeatResp Heartbeat();
}
