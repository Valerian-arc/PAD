using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using Broker;
using Common;

Console.WriteLine("=== BROKER ===");

int listenPort = args.Length > 0 ? int.Parse(args[0]) : 5050;
string storageDir = Path.Combine(AppContext.BaseDirectory, "storage");

var hub = new BrokerHub(storageDir);
var worker = new DeliveryWorker(hub, TimeSpan.FromSeconds(1));

using var cts = new CancellationTokenSource();
Task workerTask = worker.RunAsync(cts.Token);

// IPAddress.Any (0.0.0.0): acceptă conexiuni de pe orice interfață de rețea,
// nu doar de pe 127.0.0.1 - necesar cînd Sender/Receiver rulează pe alte PC-uri.
TcpListener listener = new(IPAddress.Any, listenPort);
listener.Start();

Console.WriteLine($"Broker ascultă pe portul {listenPort}, pe toate interfețele rețelei.");
Console.WriteLine($"Storage (persistent, XML): {storageDir}");
Console.WriteLine("Sender-ii publică mesaje pe un subiect (topic); Receiver-ii se abonează la un subiect.");
Console.WriteLine();
Console.WriteLine("Pentru a porni Sender/Receiver de pe alt calculator din aceeași rețea, foloseste:");
Console.WriteLine($"  dotnet run -- <adresa-ip-a-acestui-broker> {listenPort}");
Console.WriteLine("Adresa IP a acestui calculator în rețeaua locală:");
foreach (string ip in GetLocalIPv4Addresses())
{
    Console.WriteLine($"  - {ip}");
}
Console.WriteLine();

static IEnumerable<string> GetLocalIPv4Addresses()
{
    foreach (NetworkInterface nic in NetworkInterface.GetAllNetworkInterfaces())
    {
        if (nic.OperationalStatus != OperationalStatus.Up || nic.NetworkInterfaceType == NetworkInterfaceType.Loopback)
        {
            continue;
        }

        foreach (UnicastIPAddressInformation addr in nic.GetIPProperties().UnicastAddresses)
        {
            if (addr.Address.AddressFamily == AddressFamily.InterNetwork)
            {
                yield return addr.Address.ToString();
            }
        }
    }
}

// Bucla de acceptare nu se blochează niciodată: fiecare conexiune e tratată
// pe un Task separat, deci Brokerul procesează cereri concurent.
while (true)
{
    TcpClient client = await listener.AcceptTcpClientAsync();
    _ = HandleConnectionAsync(client);
}

async Task HandleConnectionAsync(TcpClient client)
{
    string endpoint = client.Client.RemoteEndPoint?.ToString() ?? "necunoscut";
    NetworkStream stream = client.GetStream();

    Message? first;
    try
    {
        first = await MessageProtocol.ReceiveMessageAsync(stream);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[!] Prim mesaj invalid de la {endpoint}: {ex.Message}");
        client.Dispose();
        return;
    }

    if (first is null)
    {
        client.Dispose();
        return;
    }

    bool isSubscribe = first.Type == MessageType.Command &&
                        first.Payload.Equals("SUBSCRIBE", StringComparison.OrdinalIgnoreCase);

    if (isSubscribe)
    {
        await RunAsSubscriberAsync(client, stream, first.Topic, endpoint);
    }
    else
    {
        await RunAsPublisherAsync(client, stream, first, endpoint);
    }
}

async Task RunAsSubscriberAsync(TcpClient client, NetworkStream stream, string topic, string endpoint)
{
    var subscriber = new SubscriberConnection
    {
        Client = client,
        Stream = stream,
        Topic = topic,
        Endpoint = endpoint
    };

    Guid id = await hub.RegisterSubscriberAsync(subscriber);

    try
    {
        // Conexiunea rămîne deschisă doar pentru livrare (push de la Broker).
        // Citim în continuare ca să detectăm rapid deconectarea (EOF) sau
        // date neașteptate, fără să blocăm restul Brokerului.
        while (true)
        {
            Message? msg = await MessageProtocol.ReceiveMessageAsync(stream);
            if (msg is null)
            {
                break; // subscriber deconectat
            }
            // Ignorăm orice altceva trimis de subscriber după abonare.
        }
    }
    catch
    {
        // deconectare / eroare de rețea - tratată mai jos în finally
    }
    finally
    {
        hub.RemoveSubscriber(id);
        client.Dispose();
    }
}

async Task RunAsPublisherAsync(TcpClient client, NetworkStream stream, Message firstMessage, string endpoint)
{
    Console.WriteLine($"[+] Publisher conectat: {endpoint}");

    try
    {
        Message? pending = firstMessage;

        while (true)
        {
            Message? message;
            if (pending is not null)
            {
                message = pending;
                pending = null;
            }
            else
            {
                try
                {
                    message = await MessageProtocol.ReceiveMessageAsync(stream);
                }
                catch (Exception ex)
                {
                    // Politică de livrare pentru mesaje invalide (cerința 1.1.4):
                    // logăm și continuăm să ascultăm pe aceeași conexiune,
                    // fără a reprocesa mesajul anterior.
                    Console.WriteLine($"[!] Mesaj invalid primit de la {endpoint}: {ex.Message}");
                    continue;
                }
            }

            if (message is null)
            {
                break; // publisher deconectat
            }

            Console.WriteLine($"[>] Mesaj primit [Id={message.Id}, Subiect={message.Topic}] de la \"{message.Sender}\": \"{message.Payload}\"");
            await hub.PublishAsync(message);
        }
    }
    finally
    {
        Console.WriteLine($"[-] Publisher deconectat: {endpoint}");
        client.Dispose();
    }
}
