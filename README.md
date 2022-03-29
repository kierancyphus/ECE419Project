# ECE419 Project
This project uses gradle, but if it's not installed on the machine can always use the `./gradlew` build script.

## Running
### Server
`./gradlew run --args="server 50000 1000 LRU"`
### Client
`./gradlew run --args="client servers.cfg" --console=plain`
### ECS UI
`./gradlew run --args="ecsUI" --console=plain`
### ECS
`./gradlew run --args="ecs servers.cfg LFU 1000 50100"`

## To build a jar file:
```./gradlew shadowJar```

The file can be found at `build/libs/ece419-1.3-SNAPSHOT-all.jar` and can be called like in M1

## Testing
`./gradlew clean test`