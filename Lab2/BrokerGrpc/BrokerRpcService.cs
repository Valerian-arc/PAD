using Grpc.Core;
using MessageBrokerGrpc;

namespace BrokerGrpc;

/// <summary>
/// Implementarea serviciului gRPC. Aceleași responsabilități ca Broker-ul
/// din Partea 1 (primire, rutare, transmitere - vezi "Considerații
/// generale" din enunț), dar expuse ca RPC-uri în loc de socket brut.
/// </summary>
public sealed class BrokerRpcService : MessageBrokerService.MessageBrokerServiceBase
{
    private const int MaxTopicLength = 100;
    private const int MaxNameLength = 50;
    private const int MaxPayloadLength = 64 * 1024;

    private readonly GrpcMessageHub _hub;
    private readonly IHostApplicationLifetime _lifetime;

    public BrokerRpcService(GrpcMessageHub hub, IHostApplicationLifetime lifetime)
    {
        _hub = hub;
        _lifetime = lifetime;
    }

    public override Task<ConnectReply> Connect(ConnectRequest request, ServerCallContext context)
    {
        string name = ValidateName(request.Name, context);
        Console.WriteLine($"[+] Publisher conectat: \"{name}\" ({FormatPeer(context.Peer)})");
        return Task.FromResult(new ConnectReply { Ok = true });
    }

    public override Task<ConnectReply> Disconnect(ConnectRequest request, ServerCallContext context)
    {
        string name = ValidateName(request.Name, context);
        Console.WriteLine($"[-] Publisher deconectat: \"{name}\" ({FormatPeer(context.Peer)})");
        return Task.FromResult(new ConnectReply { Ok = true });
    }

    public override async Task<PublishReply> Publish(PublishRequest request, ServerCallContext context)
    {
        // Validarea datelor primite (cerința 2.3). Un mesaj invalid e respins
        // cu InvalidArgument - Sender-ul primește motivul, iar Broker-ul nu cade
        // (politica pentru mesaje invalide, cerința 1.1.4).
        string topic = ValidateTopic(request.Topic, context);
        string sender = ValidateName(request.Sender, context);

        if (string.IsNullOrWhiteSpace(request.Payload))
        {
            throw Invalid(context, "Conținutul mesajului (payload) este gol.");
        }

        if (request.Payload.Length > MaxPayloadLength)
        {
            throw Invalid(context, $"Mesajul depășește {MaxPayloadLength} caractere.");
        }

        // Broker-ul parsează tipul mesajului (cerința 1.1). Proto3 acceptă și
        // valori numerice necunoscute pentru enum, deci le verificăm explicit.
        MessageType type = request.Type == MessageType.Unspecified ? MessageType.Text : request.Type;
        if (!Enum.IsDefined(type))
        {
            throw Invalid(context, $"Tip de mesaj necunoscut: {(int)request.Type}.");
        }

        var messageEvent = new MessageEvent
        {
            Id = Guid.NewGuid().ToString(),
            Topic = topic,
            Sender = sender,
            Payload = request.Payload,
            Type = type,
            Timestamp = DateTime.UtcNow.ToString("O")
        };

        Console.WriteLine($"[>] Publish primit [{type}] pe '{topic}' de la \"{sender}\" ({FormatPeer(context.Peer)}): \"{request.Payload}\"");

        // Stocăm mesajul (memorie + disc); livrarea efectivă e făcută periodic
        // de GrpcDeliveryWorker, deci mesajul nu se pierde dacă momentan nu
        // există niciun subscriber pe acest subiect.
        await _hub.PublishAsync(messageEvent);

        return new PublishReply { Accepted = true, MessageId = messageEvent.Id };
    }

    public override async Task Subscribe(
        SubscribeRequest request,
        IServerStreamWriter<MessageEvent> responseStream,
        ServerCallContext context)
    {
        string topic = ValidateTopic(request.Topic, context);
        string name = string.IsNullOrWhiteSpace(request.Name) ? "anonim" : ValidateName(request.Name, context);
        string peer = FormatPeer(context.Peer);

        var subscription = new GrpcMessageHub.Subscription
        {
            Topic = topic,
            Name = name,
            Peer = peer,
            Stream = responseStream
        };

        // Trimitem imediat antetele răspunsului: Receiver-ul le așteaptă ca
        // confirmare că abonarea a fost acceptată.
        await context.WriteResponseHeadersAsync(new Metadata());

        Console.WriteLine($"[pubsub] Subscriber nou \"{name}\" pe subiectul '{topic}' ({peer}).");
        Guid id = await _hub.SubscribeAsync(subscription);

        // RPC-ul rămîne deschis cît timp subscriber-ul e abonat; mesajele sînt
        // scrise pe stream de GrpcDeliveryWorker. Se încheie cînd clientul se
        // dezabonează, cînd livrarea către el eșuează, sau cînd Broker-ul se
        // oprește (ApplicationStopping vine înainte ca Kestrel să închidă
        // conexiunile, deci Receiver-ul apucă să afle).
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(
            context.CancellationToken, _lifetime.ApplicationStopping, subscription.Closed.Token);

        try
        {
            await Task.Delay(Timeout.Infinite, stop.Token);
        }
        catch (OperationCanceledException)
        {
            // încheiere normală a abonării
        }
        finally
        {
            _hub.Unsubscribe(id);
            await subscription.CompleteAsync();
            Console.WriteLine($"[pubsub] Subscriber \"{name}\" deconectat de la subiectul '{topic}' ({peer}).");
        }
    }

    /// <summary>
    /// Subiectul devine și nume de folder în storage, deci nu poate conține
    /// caractere invalide pentru fișiere.
    /// </summary>
    private static string ValidateTopic(string raw, ServerCallContext context)
    {
        string topic = raw.Trim();
        if (topic.Length == 0)
        {
            throw Invalid(context, "Subiectul (topic) este obligatoriu.");
        }

        if (topic.Length > MaxTopicLength)
        {
            throw Invalid(context, $"Subiectul depășește {MaxTopicLength} caractere.");
        }

        if (topic is "." or ".." || topic.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0 || topic.Contains('\\'))
        {
            throw Invalid(context, $"Subiectul '{topic}' conține caractere nepermise.");
        }

        return topic;
    }

    private static string ValidateName(string raw, ServerCallContext context)
    {
        string name = raw.Trim();
        if (name.Length == 0)
        {
            throw Invalid(context, "Numele este obligatoriu.");
        }

        if (name.Length > MaxNameLength)
        {
            throw Invalid(context, $"Numele depășește {MaxNameLength} caractere.");
        }

        return name;
    }

    private static RpcException Invalid(ServerCallContext context, string reason)
    {
        Console.WriteLine($"[!] Cerere invalidă de la {FormatPeer(context.Peer)}: {reason}");
        return new RpcException(new Status(StatusCode.InvalidArgument, reason));
    }

    /// <summary>
    /// gRPC nu expune socket-ul direct (ca TcpClient.RemoteEndPoint la
    /// Partea 1); adresa clientului vine în context.Peer, sub forma
    /// "ipv4:192.168.1.23:54821". Păstrăm doar partea IP:port.
    /// </summary>
    private static string FormatPeer(string peer)
    {
        int colon = peer.IndexOf(':');
        return colon >= 0 && peer.StartsWith("ipv") ? peer[(colon + 1)..] : peer;
    }
}
