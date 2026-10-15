using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using BrokerGrpc;
using Microsoft.AspNetCore.Server.Kestrel.Core;

var builder = WebApplication.CreateBuilder(args);

// Argumente: [port] [--demo-dlq], în orice ordine.
int port = args.Select(a => int.TryParse(a, out int p) ? p : 0).FirstOrDefault(p => p > 0, 5091);
bool demoDlq = args.Contains("--demo-dlq");

// Ascultăm HTTP/2 fără TLS (h2c) ca să evităm certificate dev pe Mac.
// IPAddress.Any (0.0.0.0), NU ListenLocalhost: astfel Broker-ul acceptă
// conexiuni și de pe alte calculatoare din aceeași rețea (nu doar de pe
// 127.0.0.1), necesar cînd Sender/Receiver rulează pe alte PC-uri.
builder.WebHost.ConfigureKestrel(options =>
{
    options.Listen(IPAddress.Any, port, listenOptions => listenOptions.Protocols = HttpProtocols.Http2);
});

builder.Services.AddGrpc();
string storageDir = Path.Combine(AppContext.BaseDirectory, "storage");
builder.Services.AddSingleton(new GrpcMessageHub(storageDir) { DemoDlq = demoDlq });
builder.Services.AddHostedService<GrpcDeliveryWorker>();

WebApplication app = builder.Build();

app.MapGrpcService<BrokerRpcService>();
app.MapGet("/", () => "Broker gRPC pornit. Conectează-te cu un client gRPC (SenderGrpc / ReceiverGrpc).");

Console.WriteLine("=== BROKER gRPC ===");
Console.WriteLine($"Storage (persistent, protobuf binar): {storageDir}");
Console.WriteLine($"Ascultă pe portul {port} (HTTP/2 cleartext), pe toate interfețele rețelei.");
Console.WriteLine($"  - de pe acest calculator: http://127.0.0.1:{port}");
foreach (string ip in GetLocalIPv4Addresses())
{
    Console.WriteLine($"  - de pe alt calculator din rețea: http://{ip}:{port}");
}
Console.WriteLine("(Sender-ul/Receiver-ul de pe alte PC-uri trebuie pornite cu acea adresă ca argument.)");
if (demoDlq)
{
    Console.WriteLine();
    Console.WriteLine($"[demo] Mod demonstrativ DLQ activ: mesajele care încep cu '{GrpcMessageHub.DemoDlqMarker}' nu pot fi livrate");
    Console.WriteLine($"[demo] și ajung în Dead Letter Queue după {GrpcMessageHub.MaxDeliveryAttempts} încercări (cu un Receiver abonat).");
}

app.Run();

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
