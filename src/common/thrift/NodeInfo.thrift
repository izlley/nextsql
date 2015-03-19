namespace java org.apache.nextsql.thrift
namespace cpp nextsql.thrift

struct TNetworkAddress {
  1: required string hostname
  2: required i32 rsm_port
  3: required i32 paxos_port
}

struct TNodeInfo {
  1: required map<i64, TNetworkAddress> nodeidinfo_map
  2: required i64 version
}
