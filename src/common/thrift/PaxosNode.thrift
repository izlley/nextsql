
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
  2: required i32 rsm_port
  3: required i32 paxos_port
}

struct TBallotNum {
  1: required i64 id
  2: required i64 nodeid
}

struct TAcceptedValue {
  1: required TBallotNum ballot_num
  2: required i64 slot_num
  3: required Operation.TOperation operation
}

//
// LeaderPropose()
//
struct TLeaderProposeReq {
  1: required i64 blockId
  2: required TBallotNum ballot_num
}

struct TLeaderProposeResp {
  1: required Status.TStatus status
  2: required TBallotNum ballot_num
}

//
// AcceptorPhaseOne()
//
struct TAcceptorPhaseOneReq {
  1: required i64 blockId
  2: required TBallotNum ballot_num
}

struct TAcceptorPhaseOneResp {
  1: required Status.TStatus status
  2: required TBallotNum ballot_num
  3: required list<TAcceptedValue> accepted_values
}

//
// LeaderAccept()
//
struct TLeaderAcceptReq {
  1: required i64 blockId
  2: required i64 slot_num
  3: required Operation.TOperation operation
}

struct TLeaderAcceptResp {
  1: required Status.TStatus status
}

//
// AcceptorPhaseTwo()
//
struct TAcceptorPhaseTwoReq {
  1: required i64 blockId
  2: required TBallotNum ballot_num
  3: required i64 slot_num
  4: required Operation.TOperation operation
}

struct TAcceptorPhaseTwoResp {
  1: required Status.TStatus status
  2: required TBallotNum ballot_num
}

//
// Heartbeat()
//
struct THeartbeatResp {
  1: required Status.TStatus status
}

service PaxosService {
  TLeaderProposeResp LeaderPropose(1:TLeaderProposeReq req);
  
  TLeaderAcceptResp LeaderAccept(1:TLeaderAcceptReq req);
  
  TAcceptorPhaseOneResp AcceptorPhaseOne(1:TAcceptorPhaseOneReq req);
  
  TAcceptorPhaseTwoResp AcceptorPhaseTwo(1:TAcceptorPhaseTwoReq req);
  
  THeartbeatResp Heartbeat();
}
