namespace java org.apache.nextsql.thrift
namespace cpp nextsql.thrift

include "NodeInfo.thrift"
include "Operation.thrift"

struct TLeaderRep {
  // replica id = <blockId>-<random-int>
  1: required string leader_repid
  2: required NodeInfo.TNetworkAddress leader_repaddr
}

struct TClientBlockMeta {
  1: required map<string, list<string>> fileblkid_map
  2: required map<string, TLeaderRep> blkidleader_map
  3: required i64 version
}

struct TReplicaMeta {
  1: required list<Operation.TRepNode> replicas
  2: required i32 leader_idx
}

struct TBlkIdRepMeta {
  1: required map<string, TReplicaMeta> blkidrepmeta_map
  2: required i64 version
}