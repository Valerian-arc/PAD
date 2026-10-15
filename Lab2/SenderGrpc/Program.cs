using Grpc.Core;
using Grpc.Net.Client;
using MessageBrokerGrpc;

Console.WriteLine("=== SENDER (gRPC) ===");

// Permite client gRPC peste HTTP simplu (h2c), fără certificat TLS - la fel
// ca serverul, pentru testare locală simplă.
AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

string address = args.Length > 0 ? args[0] : "http://127.0.0.1:5091";

using GrpcChannel channel = GrpcChannel.ForAddress(address);
var client = new MessageBrokerService.MessageBrokerServiceClient(channel);

string senderName = ReadName(args.Length > 1 ? args[1] : null, $"Sender-gRPC-{Environment.ProcessId}");

// GrpcChannel nu se conectează efectiv decît la primul apel RPC, deci facem
// un apel Connect imediat: Broker-ul ne înregistrează (nume + IP:port), iar
// noi aflăm de la început dacă Broker-ul e pornit.
try
{
    await client.ConnectAsync(new ConnectRequest { Name = senderName });
}
catch (RpcException ex)
{
    Console.WriteLine($"[EROARE] Nu mă pot conecta la Broker-ul gRPC ({address}): {ex.Status.Detail}");
    return;
}

Console.WriteLine($"Conectat la Broker gRPC ({address}).");

Console.Write("Subiect (topic) pe care publici mesajele (fără semnele < >): ");
string topic = NormalizeTopic(Console.ReadLine());

Console.WriteLine();
Console.WriteLine($"Publici pe subiectul '{topic}'. Scrie un text și apasă Enter ('exit' pentru ieșire).");
Console.WriteLine();

while (true)
{
    Console.Write("> ");
    string? input = Console.ReadLine();
    if (input == null) break;
    if (input.Length == 0) continue;
    if (input.Equals("exit", StringComparison.OrdinalIgnoreCase)) break;

    try
    {
        PublishReply reply = await client.PublishAsync(new PublishRequest
        {
            Topic = topic,
            Sender = senderName,
            Payload = input,
            Type = MessageType.Text
        });

        Console.WriteLine($"  [OK] acceptat de Broker (Id={reply.MessageId})");
    }
    catch (RpcException ex)
    {
        // Mesaj respins de Broker (validare) sau Broker indisponibil.
        Console.WriteLine($"  [EROARE] {ex.Status.Detail}");
    }
}

try
{
    await client.DisconnectAsync(new ConnectRequest { Name = senderName });
}
catch (RpcException)
{
    // Broker-ul poate fi deja oprit - nu e nimic de făcut.
}

Console.WriteLine("Sender gRPC oprit.");
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
