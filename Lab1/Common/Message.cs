namespace Common;

/// <summary>
/// Tipul mesajului transmis prin agent. Brokerul poate folosi acest cîmp
/// pentru rutare (conform cerinței 1.1.4 din enunț: parsarea mesajului
/// pentru a afla tipul lui).
/// </summary>
public enum MessageType
{
    Text,
    Command,
    Notification
}

/// <summary>
/// Structura mesajului transmis între Sender -> Broker -> Receiver.
/// Este serializat/deserializat în format JSON (conform cerinței 1.1.1).
/// </summary>
public class Message
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public MessageType Type { get; set; }
    public string Sender { get; set; } = string.Empty;

    /// <summary>
    /// Subiectul mesajului (topic) - identificatorul logic al canalului de
    /// comunicare folosit de Broker pentru rutare Publisher/Subscriber.
    /// </summary>
    public string Topic { get; set; } = "default";

    public string Payload { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;

    /// <summary>
    /// Numărul de încercări de livrare deja efectuate de Broker. După
    /// BrokerHub.MaxDeliveryAttempts încercări eșuate, mesajul e mutat
    /// în Dead Letter Queue.
    /// </summary>
    public int DeliveryAttempts { get; set; }
}
