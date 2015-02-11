# nextsql
The next generation of database

## Multi-paxos
Implemented multi-paxos for data replication and transaction control.

```
Message flow: Multi-Paxos Collapsed Roles, start
(first instance with new leader)

Client      Servers
   |         |  |  | --- First Request ---
   X-------->|  |  |  Request
   |         X->|->|  Prepare(N)
   |         |<-X--X  Promise(N,I,{Va,Vb})
   |         X->|->|  Accept!(N,I,Vn)
   |         |<-X--X  Accepted(N,I)
   |<--------X  |  |  Response
   |         |  |  |

Message flow: Multi-Paxos Collapsed Roles, steady state
(subsequent instances with same leader)

Client      Servers
   X-------->|  |  |  Request
   |         X->|->|  Accept!(N,I+1,W)
   |         |<-X--X  Accepted(N,I+1)
   |<--------X  |  |  Response
   |         |  |  |
```
