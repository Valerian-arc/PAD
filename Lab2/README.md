# Lab 2 - Agent de mesagerie. Invocarea la distanță (gRPC)

Partea 2 a proiectului: același Broker Publisher/Subscriber ca în `Lab1/`,
dar comunicarea prin socketuri e înlocuită cu **gRPC**. Proiect separat de
Lab 1 - se deschide și rulează independent (`MessageBrokerGrpc.sln`).

Aceeași arhitectură logică (Sender -> Broker -> Receiver, rutare pe
subiect), dar transportul brut peste socket e înlocuit cu **RPC-uri gRPC**,
iar reprezentarea datelor e **Protocol Buffers** (`Proto/messagebroker.proto`).

- `Connect` / `Disconnect` - RPC-uri unare prin care Sender-ul se anunță
  la pornire/ieșire. gRPC nu are un eveniment de "client conectat" (ca
  `AcceptTcpClient` la Partea 1), așa că Broker-ul află de Sender doar
  printr-un apel RPC. Broker-ul loghează numele și adresa IP:port, luată din
  `ServerCallContext.Peer`.
- `Publish` - RPC unar: Sender-ul trimite un mesaj, Broker-ul răspunde cu
  confirmare + Id generat.
- `Subscribe` - RPC cu **server streaming**: Receiver-ul deschide un flux,
  iar Broker-ul (`GrpcDeliveryWorker`) scrie pe acel flux fiecare mesaj
  publicat pe subiectul cerut.
  `SubscribeRequest` conține și numele Receiver-ului, logat de Broker
  împreună cu IP:port. Dacă Broker-ul se oprește sau cade, Receiver-ul afișează
  o eroare și se poate reabona cu `topic nume-subiect`.
- **Tipul mesajului**: `enum MessageType` (TEXT / COMMAND / NOTIFICATION) în
  proto; Broker-ul îl parsează și respinge valorile necunoscute.
- **Validarea datelor** (cerința 2.3): subiect (obligatoriu, max. 100
  caractere, fără caractere invalide pentru nume de fișier), nume (max. 50),
  payload (nevid, max. 64 KB), tip. O cerere invalidă e respinsă cu
  `StatusCode.InvalidArgument` și motivul, Sender-ul afișează eroarea, iar
  Broker-ul nu cade. Datele binare corupte sînt respinse de gRPC însuși.
- **Procesare concurentă**: fiecare RPC e servit pe propriul stream HTTP/2
  (ASP.NET Core), iar work job-ul (`GrpcDeliveryWorker`, la fiecare secundă)
  procesează topicurile concurent (`Task.WhenAll`).
- **Stocare** (`BrokerGrpc/GrpcPersistentStore.cs`): transient în colecții
  thread-safe (`ConcurrentQueue`/`ConcurrentDictionary`) + persistent pe disc,
  în format **protobuf binar**, scris asincron, sub
  `BrokerGrpc/bin/Debug/net10.0/storage/` (`history/`, `pending/`,
  `deadletter/`, ca la Partea 1). La repornirea după o cădere, mesajele
  nelivrate sînt restaurate.
- **Politica de livrare**: worker-ul scrie direct pe stream-ul gRPC al
  fiecărui subscriber (timeout 5 s). Un subscriber la care scrierea eșuează e
  deconectat; dacă niciun subscriber n-a primit mesajul, acesta revine în
  coadă (max. 3 încercări, apoi Dead Letter Queue). Fără subscriberi, mesajul
  așteaptă (pe disc) fără a consuma încercări.
- **Canale și transport**: gRPC rulează peste HTTP/2 peste TCP (livrare
  sigură, în ordine). Pe o singură conexiune TCP se multiplexează mai multe
  stream-uri; `Publish` e un canal unidirecțional Sender -> Broker (cerere +
  confirmare), `Subscribe` un canal unidirecțional Broker -> Receiver
  (server streaming), deci numărul de canale e variabil: unul per abonare.
- Serverul ascultă **HTTP/2 fără TLS** (h2c) pe `http://127.0.0.1:5091`,
  ca să eviți certificate dev pe Mac; clienții activează explicit suportul
  pentru HTTP/2 necriptat.

## Ordinea de pornire

1. `BrokerGrpc`
2. `ReceiverGrpc` - îți cere subiectul la care te abonezi
3. `SenderGrpc` - îți cere subiectul pe care publici

## Demonstrație Dead Letter Queue

1. `cd BrokerGrpc && dotnet run -- --demo-dlq`
2. `ReceiverGrpc` - abonează-te la un subiect, ex. `sport`
3. `SenderGrpc` - pe `sport`, trimite un mesaj normal, apoi unul care începe
   cu `#dlq`, ex. `#dlq acesta nu poate fi livrat`
4. Mesajul normal ajunge la Receiver. Pentru cel cu `#dlq`, Broker-ul afișează
   `[retry] încercarea 1/3`, `2/3`, apoi `[dlq] ... mutat în Dead Letter Queue`.
5. Fișierul apare în `BrokerGrpc/bin/Debug/net10.0/storage/deadletter/sport/`.
6. Repornește Broker-ul: la pornire listează mesajele din DLQ, reîncărcate de
   pe disc în memorie.

`--demo-dlq` simulează un "mesaj otrăvit" (poison message), pe care nicio
livrare nu reușește - cazul clasic pentru care există DLQ. Fără acest flag,
Broker-ul funcționează normal, iar `#dlq` nu are nicio semnificație.

## Notă despre build

La prima rulare, Rider/`dotnet` va descărca automat pachetele NuGet
(`Grpc.AspNetCore`, `Grpc.Net.Client`, `Google.Protobuf`, `Grpc.Tools`) și va
genera codul C# din `messagebroker.proto` - ai nevoie de conexiune la
internet pentru acest pas (o singură dată, la restore).

## Rulare pe calculatoare diferite

`BrokerGrpc` ascultă pe toate interfețele de rețea și afișează la pornire
adresele IP. `SenderGrpc`/`ReceiverGrpc` primesc adresa completă a
Broker-ului ca argument (opțional, numele ca al doilea argument):

```
cd BrokerGrpc   && dotnet run                     # opțional: -- 5091 --demo-dlq
cd ReceiverGrpc && dotnet run -- http://192.168.1.23:5091 Ana
cd SenderGrpc   && dotnet run -- http://192.168.1.23:5091 Ion
```

Fără argumente, clienții se conectează la `http://127.0.0.1:5091`. Portul
`5091` trebuie permis de firewall pe calculatorul cu Broker-ul.

## Structura

```
MessageBrokerGrpc.sln
Proto/          - contractul messagebroker.proto (serviciu + mesaje)
BrokerGrpc/     - Broker (server gRPC, ASP.NET Core): rutare, stocare, worker, DLQ
SenderGrpc/     - Sender (client gRPC, Publish)
ReceiverGrpc/   - Receiver (client gRPC, Subscribe cu server streaming)
```
