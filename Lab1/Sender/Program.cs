using System.Net.Sockets;
using System.Text;
using Common;

Console.WriteLine("=== SENDER (Publisher) ===");

string brokerHost = args.Length > 0 ? args[0] : "127.0.0.1";
int brokerPort = args.Length > 1 ? int.Parse(args[1]) : 5050;

Console.WriteLine($"Conectare la Broker {brokerHost}:{brokerPort} ...");

using TcpClient client = new();
await client.ConnectAsync(brokerHost, brokerPort);
Console.WriteLine("Conectat la Broker.");

await using NetworkStream stream = client.GetStream();

Console.Write("Subiect (topic) pe care publici mesajele (fără semnele < >): ");
string topic = NormalizeTopic(Console.ReadLine());

Console.WriteLine();
Console.WriteLine($"Publici pe subiectul '{topic}'. Scrie un text și apasă Enter pentru a trimite.");
Console.WriteLine("Comenzi speciale:");
Console.WriteLine("  topic nume-subiect  - schimbă subiectul pe care publici, fără < > (fără reconectare)");
Console.WriteLine("  exit          - închide sender-ul");
Console.WriteLine("  invalid       - trimite date corupte (test tratare excepții la Broker)");
Console.WriteLine();

while (true)
{
    Console.Write($"[{topic}] > ");
    string? input = Console.ReadLine();
    if (input == null) break;
    if (input.Length == 0) continue;

    if (input.Equals("exit", StringComparison.OrdinalIgnoreCase))
    {
        break;
    }

    if (input.Equals("invalid", StringComparison.OrdinalIgnoreCase))
    {
        await SendCorruptedFrameAsync(stream);
        continue;
    }

    if (input.StartsWith("topic ", StringComparison.OrdinalIgnoreCase))
    {
        string candidate = NormalizeTopic(input["topic ".Length..]);
        if (candidate.Length > 0)
        {
            topic = candidate;
            Console.WriteLine($"  [OK] publici acum pe subiectul '{topic}'.");
        }
        continue;
    }

    var message = new Message
    {
        Type = MessageType.Text,
        Sender = "Sender-1",
        Topic = topic,
        Payload = input
    };

    try
    {
        await MessageProtocol.SendMessageAsync(stream, message);
        Console.WriteLine($"  [OK] mesaj trimis pe '{topic}' (Id={message.Id})");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"  [EROARE] nu s-a putut trimite mesajul: {ex.Message}");
        break;
    }
}

Console.WriteLine("Sender oprit.");
return;

// A se vedea Receiver/Program.cs - aceeași problemă: dacă utilizatorul
// tastează literal "topic <e>" (copiind placeholderul din mesajul de ajutor),
// subiectul rezultat ar fi "<e>", diferit de "e". Eliminăm < > din jurul
// numelui, dacă sînt prezente.
static string NormalizeTopic(string? raw)
{
    string t = (raw ?? string.Empty).Trim();
    if (t.Length >= 2 && t[0] == '<' && t[^1] == '>')
    {
        t = t[1..^1].Trim();
    }

    return t.Length > 0 ? t : "default";
}

static async Task SendCorruptedFrameAsync(NetworkStream stream)
{
    byte[] garbage = Encoding.UTF8.GetBytes("acesta nu este JSON valid {{{");
    byte[] lengthBytes = BitConverter.GetBytes(garbage.Length);
    await stream.WriteAsync(lengthBytes);
    await stream.WriteAsync(garbage);
    await stream.FlushAsync();
    Console.WriteLine("  [OK] mesaj invalid trimis (pentru testarea tratării excepțiilor)");
}
