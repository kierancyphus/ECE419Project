# ECE419 Project
This project uses gradle, but if it's not installed on the machine can always use the `./gradlew` build script.

## Running
Complete flow
`./gradlew clean shadowJar`

In one window
`./gradlew run --args="ecs servers.cfg LFU 1000 50100"`

In another window
`./gradlew run --args="ecsUI" --console=plain`

when the console comes up enter

`connect localhost 50100`

`add 3`

`start`

In a third window run
`./gradlew run --args="client apiGateway.cfg" --console=plain`

within the window you need to login with

`login test eubhadfbhjfd`

and then 

`put key value`

`get key`

where username is the name of a file you added under the auth folder and password is the content in it

### Server
`./gradlew run --args="server 50000 1000 LRU"`
### Client
`./gradlew run --args="client apiGateway.cfg" --console=plain`
### ECS UI
`./gradlew run --args="ecsUI" --console=plain`
### ECS
`./gradlew run --args="ecs servers.cfg LFU 1000 50100"`
### Start ECS nodes
In ECS UI, call ```connect localhost 50100``` to connect to ECS. 

To add servers, in ECS UI call ```add n``` where ```n``` is the number of servers we want to add.

To start these servers, in ECS UI call ```start```.

To remove servers, in ECS UI call ```remove idx``` where ```idx``` is the index of the server node.

To stop or shutdown ECS, call ```stop``` and ```shutdown```.
### Api Gateway
I decided to get rid of the host since we do everything on localhost anyway.
`./gradlew run --args="api 50200"`
## To build a jar file:
```./gradlew shadowJar```

The file can be found at `build/libs/ece419-1.3-SNAPSHOT-all.jar` and can be called like in M1

## Testing
`./gradlew clean test`

## Cleaning
To remove straggling servers, you can run
```kill -9 `jps | grep "ece419-1.3-SNAPSHOT-all.jar" | cut -d " " -f 1````