using System.Collections.Concurrent;
using Common;

namespace Broker;

/// <summary>
/// Nivelul de rutare Publisher/Subscriber al Brokerului.
///
/// - O COADĂ SEPARATĂ PENTRU FIECARE TOPIC (queue1 -> topic1, queue2 -> topic2, ...),
///   implementată cu ConcurrentQueue (colecție thread-safe .NET, cerința 1.3.1).
/// - ISTORIC PERSISTENT per topic: fiecare mesaj publicat e salvat pe disc și
///   NU se șterge, astfel un subscriber care se abonează mai tîrziu primește
///   toată "conversația" de pînă atunci.
/// - DEAD LETTER QUEUE: un mesaj care nu poate fi livrat nici după
///   MaxDeliveryAttempts încercări e mutat într-o zonă separată pe disc.
/// </summary>
public sealed class BrokerHub
{
    public const int MaxDeliveryAttempts = 3;

    private readonly ConcurrentDictionary<string, ConcurrentQueue<Message>> _queues = new();
    private readonly ConcurrentDictionary<Guid, SubscriberConnection> _subscribers = new();

    public string StorageDir { get; }

    public BrokerHub(string storageDir)
    {
        StorageDir = storageDir;
        Directory.CreateDirectory(StorageDir);
        RestorePending();
    }

    private void RestorePending()
    {
        int restored = 0;
        foreach (Message message in PersistentStore.LoadAllPending(StorageDir))
        {
            GetQueue(message.Topic).Enqueue(message);
            restored++;
        }

        if (restored > 0)
        {
            Console.WriteLine($"[storage] {restored} mesaj(e) nelivrate restaurate de pe disc.");
        }

        int dead = PersistentStore.CountDeadLetters(StorageDir);
        if (dead > 0)
        {
            Console.WriteLine($"[dlq] {dead} mesaj(e) se află în Dead Letter Queue.");
        }
    }

    private ConcurrentQueue<Message> GetQueue(string topic) =>
        _queues.GetOrAdd(topic, _ => new ConcurrentQueue<Message>());

    public ConcurrentQueue<Message> GetQueueForTopic(string topic) => GetQueue(topic);

    public List<string> Topics => _queues.Keys.ToList();

    /// <summary>
    /// Publicarea unui mesaj: intră în coada topicului (pentru livrare către
    /// subscriberii activi) ȘI în istoricul persistent al topicului (pentru
    /// subscriberii viitori).
    /// </summary>
    public async Task PublishAsync(Message message)
    {
        if (string.IsNullOrWhiteSpace(message.Topic))
        {
            message.Topic = "default";
        }

        message.DeliveryAttempts = 0;

        GetQueue(message.Topic).Enqueue(message);
        await PersistentStore.AppendHistoryAsync(StorageDir, message);
        await PersistentStore.SavePendingAsync(StorageDir, message);

        Console.WriteLine($"[queue:{message.Topic}] Mesaj [Id={message.Id}] adăugat în coadă + istoric.");
    }

    /// <summary>
    /// Înregistrează un subscriber și îi trimite întregul istoric al topicului
    /// (mesajele publicate înainte ca el să se conecteze).
    /// </summary>
    public async Task<Guid> RegisterSubscriberAsync(SubscriberConnection subscriber)
    {
        Guid id = Guid.NewGuid();
        _subscribers[id] = subscriber;
        GetQueue(subscriber.Topic);

        Console.WriteLine($"[pubsub] Subscriber nou pe topicul '{subscriber.Topic}' ({subscriber.Endpoint}).");

        List<Message> history = PersistentStore.LoadHistory(StorageDir, subscriber.Topic);
        if (history.Count == 0)
        {
            return id;
        }

        Console.WriteLine($"[history] Trimit {history.Count} mesaj(e) din istoricul topicului '{subscriber.Topic}'.");

        foreach (Message old in history)
        {
            if (!subscriber.MarkDelivered(old.Id))
            {
                continue; // deja trimis
            }

            try
            {
                await MessageProtocol.SendMessageAsync(subscriber.Stream, old);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[!] Nu s-a putut trimite istoricul către {subscriber.Endpoint}: {ex.Message}");
                break;
            }
        }

        return id;
    }

    public void RemoveSubscriber(Guid id)
    {
        if (_subscribers.TryRemove(id, out SubscriberConnection? subscriber))
        {
            Console.WriteLine($"[pubsub] Subscriber deconectat de la topicul '{subscriber.Topic}' ({subscriber.Endpoint}).");
        }
    }

    public List<KeyValuePair<Guid, SubscriberConnection>> GetSubscribers(string topic) =>
        _subscribers.Where(kv => kv.Value.Topic == topic).ToList();

    /// <summary>
    /// Mută un mesaj în Dead Letter Queue după epuizarea încercărilor de livrare.
    /// </summary>
    public async Task MoveToDeadLetterAsync(Message message, string reason)
    {
        await PersistentStore.SaveDeadLetterAsync(StorageDir, message);
        PersistentStore.DeletePending(StorageDir, message);
        Console.WriteLine($"[dlq] Mesaj [Id={message.Id}] mutat în Dead Letter Queue după {message.DeliveryAttempts} încercări. Motiv: {reason}");
    }
}
