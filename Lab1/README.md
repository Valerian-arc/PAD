# Lab 1: TCP Message Broker

Plain socket implementation of a topic-based publisher/subscriber message broker.
This folder contains the Common protocol library and the Broker, Sender, and
Receiver console applications. The gRPC lab is not included.

## Run

Open `MessageBrokerLab.sln` in Rider or build it with the .NET 10 SDK. Start the
applications in this order:

1. `Broker`
2. `Receiver` (enter the topic to subscribe to)
3. `Sender` (enter the topic to publish to)

The default broker address is `127.0.0.1:5050`. Sender and Receiver accept the
broker host and port as command-line arguments, for example:

```sh
dotnet run --project Sender -- 192.168.1.23 5050
dotnet run --project Receiver -- 192.168.1.23 5050
```
