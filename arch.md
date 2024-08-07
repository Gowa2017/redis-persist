```plantuml
@startuml RP
!include <C4/C4_Dynamic>
Person(user, "Mantainer")
Container_Ext(redis, "Redis Server", "Redis")
Container_Ext(skynet, "Skynet", "C/Lua")
Container_Boundary(rp, "Redis persist to leveldb\n First skynet read data from Redis, if there is not data, then read from leveldb by agent."){
    Component(monitor, "Monitor", "Go/Tcp","Listen redis data changes, and push to the queue")
    Component(cmd, "Command Service", "Go/Tcp","We can receive command and sync data manualy.")
    Component(agent, "Agent", "Go/Tcp","Use to read data from leveldb and return to the skynet service")
    Component(db, "Leveldb Database", "LevelDb","Persist data")
    Component(queue, "Channel Queue", "Go")
    Component(storer, "Storer","Go","Read data from queue and persist to leveldb")
    Component(context, "Context", "Go","Manage this components")
}
Rel_L(monitor, redis, "subscribe")
Rel_R(redis, monitor, "notify")
Rel_R(monitor, queue, "push")
Rel_R(queue, storer,"read")
Rel_D(storer, db, "save")
Rel_U(skynet, agent, "get")
Rel_U(agent, db, "read")
Rel_U(user, cmd, "connect")
Rel_U(cmd, context, "use")
Rel_L(context, redis, "read")
Rel_U(context, queue, "sync")
' Lay_L(context, storer)
@enduml
```
