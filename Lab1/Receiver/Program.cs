using System.Net.Sockets;
using Common;

Console.WriteLine("=== RECEIVER (Subscriber) ===");

string brokerHost = args.Length > 0 ? args[0] : "127.0.0.1";
int brokerPort = args.Length > 1 ? int.Parse(args[1]) : 5050;

Console.Write("Subiect (topic) la care te abonezi (fără semnele < >): ");
string topic = NormalizeTopic(Console.ReadLine());

while (true)
{
    string sessionTopic = topic;
    string? nextTopic = null;

    using TcpClient client = new();
    await client.ConnectAsync(brokerHost, brokerPort);
    await using NetworkStream stream = client.GetStream();

    var subscribeRequest = new Message
    {
        Type = MessageType.Command,
        Sender = "Receiver-1",
        Topic = sessionTopic,
        Payload = "SUBSCRIBE"
    };
    await MessageProtocol.SendMessageAsync(stream, subscribeRequest);

    Console.WriteLine();
    Console.WriteLine($"Abonat la subiectul '{sessionTopic}'. Aștept mesaje de la Broker...");
    Console.WriteLine("Comenzi (scrie și apasă Enter, în orice moment):");
    Console.WriteLine("  topic nume-subiect  - te dezabonezi și te abonezi la alt subiect (fără < >)");
    Console.WriteLine("  exit                - închide Receiver-ul");
    Console.WriteLine();

    using var cts = new CancellationTokenSource();
    Task receiveTask = ReceiveLoopAsync(stream, cts.Token);

    while (true)
    {
        string? command = Console.ReadLine();
        if (command is null)
        {
            break; // EOF (ex. redirecționare de input) - ieșim complet
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

    cts.Cancel();
    client.Close(); // deblochează citirea din stream ca să se termine receiveTask
    try
    {
        await receiveTask;
    }
    catch
    {
        // ignorăm excepțiile provocate de închiderea voită a conexiunii
    }

    if (nextTopic is null)
    {
        break; // comandă 'exit' sau EOF - ieșim din tot programul
    }

    topic = nextTopic;
    Console.WriteLine($"Schimb abonarea pe subiectul '{topic}' ...");
}

Console.WriteLine("Receiver oprit.");
return;

// Instrucțiunile afișează comanda ca "topic <nume>", iar unii utilizatori
// tastează literal parantezele unghiulare (ex. "topic <e>"), rezultînd un
// subiect diferit de cel dorit (ex. "<e>" în loc de "e") - aparent "portaluri"
// diferite. Normalizăm: eliminăm < > din jurul numelui, dacă sînt prezente.
static string NormalizeTopic(string? raw)
{
    string t = (raw ?? string.Empty).Trim();
    if (t.Length >= 2 && t[0] == '<' && t[^1] == '>')
    {
        t = t[1..^1].Trim();
    }

    return t.Length > 0 ? t : "default";
}

static async Task ReceiveLoopAsync(NetworkStream stream, CancellationToken cancellationToken)
{
    try
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            Message? message;
            try
            {
                message = await MessageProtocol.ReceiveMessageAsync(stream);
            }
            catch
            {
                break; // conexiune închisă (voit, la schimbarea subiectului, sau de Broker)
            }

            if (message is null)
            {
                Console.WriteLine("Broker a închis conexiunea.");
                break;
            }

            Console.WriteLine("========================================");
            Console.WriteLine($"  Id:        {message.Id}");
            Console.WriteLine($"  Subiect:   {message.Topic}");
            Console.WriteLine($"  De la:     {message.Sender}");
            Console.WriteLine($"  Timestamp: {message.Timestamp:HH:mm:ss}");
            Console.WriteLine($"  Conținut:  {message.Payload}");
            Console.WriteLine("========================================");
            Console.WriteLine();
        }
    }
    catch (ObjectDisposedException)
    {
        // stream-ul a fost închis de firul principal - ieșire normală
    }
}
