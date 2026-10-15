using Grpc.Core;
using Grpc.Net.Client;
using MessageBrokerGrpc;

Console.WriteLine("=== RECEIVER (gRPC) ===");

AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

string address = args.Length > 0 ? args[0] : "http://127.0.0.1:5091";

using GrpcChannel channel = GrpcChannel.ForAddress(address);
var client = new MessageBrokerService.MessageBrokerServiceClient(channel);

string receiverName = ReadName(args.Length > 1 ? args[1] : null, $"Receiver-gRPC-{Environment.ProcessId}");

Console.Write("Subiect (topic) la care te abonezi (fără semnele < >): ");
string topic = NormalizeTopic(Console.ReadLine());

while (true)
{
    string sessionTopic = topic;
    string? nextTopic = null;

    using var callCts = new CancellationTokenSource();
    AsyncServerStreamingCall<MessageEvent> call =
        client.Subscribe(new SubscribeRequest { Topic = sessionTopic, Name = receiverName }, cancellationToken: callCts.Token);

    Console.WriteLine();
    Console.WriteLine("Comenzi (scrie și apasă Enter, în orice moment):");
    Console.WriteLine("  topic nume-subiect  - te dezabonezi și te abonezi la alt subiect (fără < >)");
    Console.WriteLine("  exit                - închide Receiver-ul");
    Console.WriteLine();

    Task receiveTask = ReceiveLoopAsync(call, address, sessionTopic, callCts.Token);

    while (true)
    {
        string? command = Console.ReadLine();
        if (command is null)
        {
            break; // EOF - ieșim complet
        }

        if (command.Equals("exit", StringComparison.OrdinalIgnoreCase))
        {
            break;
        }

        if (command.StartsWith("topic ", StringComparison.OrdinalIgnoreCase))
        {
            string candidate = NormalizeTopic(command["topic ".Length..]);
            if (candidate.Length > 0)
            {
                nextTopic = candidate;
                break;
            }
        }

        Console.WriteLine("Comandă necunoscută. Folosește 'topic nume-subiect' (fără < >) sau 'exit'.");
    }

    callCts.Cancel(); // anulează stream-ul curent
    try
    {
        await receiveTask;
    }
    catch
    {
        // ignorăm excepțiile provocate de anularea voită
    }
    call.Dispose();

    if (nextTopic is null)
    {
        break; // 'exit' sau EOF - ieșim din tot programul
    }

    topic = nextTopic;
    Console.WriteLine($"Schimb abonarea pe subiectul '{topic}' ...");
}

Console.WriteLine("Receiver gRPC oprit.");
return;

// Vezi Receiver/Program.cs (versiunea TCP) - aceeași normalizare, pentru
// cazul în care utilizatorul tastează literal "topic <e>".
static string NormalizeTopic(string? raw)
{
    string t = (raw ?? string.Empty).Trim();
    if (t.Length >= 2 && t[0] == '<' && t[^1] == '>')
    {
        t = t[1..^1].Trim();
    }

    return t.Length > 0 ? t : "default";
}

static async Task ReceiveLoopAsync(
    AsyncServerStreamingCall<MessageEvent> call, string address, string topic, CancellationToken cancellationToken)
{
    try
    {
        // Broker-ul trimite antetele imediat ce acceptă abonarea - astfel
        // aflăm sigur dacă sîntem conectați (GrpcChannel e "leneș" și nu se
        // conectează pînă la primul apel).
        await call.ResponseHeadersAsync;
        Console.WriteLine($"Conectat la Broker gRPC ({address}), abonat la subiectul '{topic}'. Aștept mesaje...");
        Console.WriteLine();

        await foreach (MessageEvent evt in call.ResponseStream.ReadAllAsync(cancellationToken))
        {
            Console.WriteLine("========================================");
            Console.WriteLine($"  Id:        {evt.Id}");
            Console.WriteLine($"  Subiect:   {evt.Topic}");
            Console.WriteLine($"  Tip:       {evt.Type}");
            Console.WriteLine($"  De la:     {evt.Sender}");
            Console.WriteLine($"  Timestamp: {evt.Timestamp}");
            Console.WriteLine($"  Conținut:  {evt.Payload}");
            Console.WriteLine("========================================");
            Console.WriteLine();
        }

        // Stream-ul s-a încheiat fără ca noi să-l fi anulat: Broker-ul a închis
        // abonarea (de ex. a fost oprit).
        if (!cancellationToken.IsCancellationRequested)
        {
            ReportLostConnection("Broker-ul a închis stream-ul.");
        }
    }
    catch (OperationCanceledException)
    {
        // anulat voit la schimbarea subiectului sau la ieșire - normal
    }
    catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled && cancellationToken.IsCancellationRequested)
    {
        // la fel, anulare voită de pe partea clientului
    }
    catch (RpcException ex)
    {
        // Broker-ul nu e pornit, a căzut sau conexiunea s-a pierdut.
        ReportLostConnection(ex.Status.Detail);
    }
}

static void ReportLostConnection(string reason)
{
    Console.WriteLine($"[EROARE] Conexiunea cu Broker-ul s-a pierdut: {reason}");
    Console.WriteLine("Scrie 'topic nume-subiect' pentru a te reabona (după ce Broker-ul repornește) sau 'exit'.");
}

static string ReadName(string? fromArgs, string defaultName)
{
    string? name = fromArgs;
    if (string.IsNullOrWhiteSpace(name))
    {
        Console.Write($"Numele tău (Enter = {defaultName}): ");
        name = Console.ReadLine();
    }

    return string.IsNullOrWhiteSpace(name) ? defaultName : name.Trim();
}
