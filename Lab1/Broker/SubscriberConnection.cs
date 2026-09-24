using System.Net.Sockets;

namespace Broker;

/// <summary>
/// O conexiune activă de Receiver, abonată la un anumit topic.
/// </summary>
public sealed class SubscriberConnection
{
    public required TcpClient Client { get; init; }
    public required NetworkStream Stream { get; init; }
    public required string Topic { get; init; }
    public required string Endpoint { get; init; }

    /// <summary>
    /// Id-urile mesajelor deja trimise acestui subscriber (din istoric sau
    /// prin livrare curentă), ca să nu primească același mesaj de două ori.
    /// </summary>
    public HashSet<Guid> DeliveredIds { get; } = new();

    public bool MarkDelivered(Guid id)
    {
        lock (DeliveredIds)
        {
            return DeliveredIds.Add(id); // false dacă era deja livrat
        }
    }
}
