
namespace java org.apache.nextsql.thrift
namespace cpp nextsql.thrift

enum TStatusCode {
  SUCCESS,
  STILL_EXECUTING,
  CANCELLED,
  REQUESTED_WRONG_NODE,
  ERROR,
}

struct TStatus {
  1: required TStatusCode status_code
  2: optional string error_message
}
