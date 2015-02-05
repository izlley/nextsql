
namespace java org.apache.nextsql.multipaxos.thrift
namespace cpp nextsql.thrift

enum TStatusCode {
  SUCCESS,
  STILL_EXECUTING,
  CANCELLED,
  ERROR,
}

struct TStatus {
  1: required TStatusCode status_code
  2: optional string error_message
}
