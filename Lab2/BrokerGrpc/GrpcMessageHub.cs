using System.Collections.Concurrent;
using Grpc.Core;
using MessageBrokerGrpc;

namespace BrokerGrpc;

/// <summary>
/// Nivelul de rutare Publisher/Subscriber al Brokerului gRPC, cu aceleași
/// garanții ca Brokerul TCP din Partea 1:
///  - o COADĂ SEPARATĂ PER TOPIC (ConcurrentQueue - metoda transientă, 1.3.1);
///  - ISTORIC per topic, ca un subscriber nou să primească mesajele publicate
///    înainte de conectarea lui;
///  - PERSISTENȚĂ pe disc (history/pending/deadletter - metoda persistentă,
///    1.3.2), cu restaurarea mesajelor nelivrate după căderea Brokerului;
///  - DEAD LETTER QUEUE (în memorie + pe disc) pentru mesajele nelivrabile
///    după 3 încercări.
///
/// Istoricul trimis unui subscriber nou conține doar mesajele deja livrate;
/// mesajele încă în coadă (pending) îi ajung doar prin worker, care numără
/// încercările - altfel un mesaj ar fi "livrat" prin istoric și n-ar mai
/// ajunge niciodată în Dead Letter Queue.
/// </summary>
public sealed class GrpcMessageHub
{
    public const int MaxDeliveryAttempts = 3;

    /// <summary>
    /// Doar pentru demonstrație (Broker pornit cu --demo-dlq): un mesaj al cărui
    /// conținut începe cu acest marcaj e tratat ca "mesaj otrăvit" (poison
    /// message) - livrarea lui eșuează mereu, deci ajunge în Dead Letter Queue.
    /// </summary>
    public const string DemoDlqMarker = "#dlq";

    public bool DemoDlq { get; init; }

    public bool IsDemoPoison(MessageEvent message) =>
        DemoDlq && message.Payload.StartsWith(DemoDlqMarker, StringComparison.OrdinalIgnoreCase);

    private static readonly TimeSpan WriteTimeout = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Un subscriber activ: stream-ul gRPC pe care primește mesajele și
    /// evidența id-urilor deja livrate (ca să nu primească același mesaj de
    /// două ori - o dată din istoric, o dată de la worker).
    /// </summary>
    public sealed class Subscription
    {
        public required string Topic { get; init; }
        public required string Name { get; init; }
        public required string Peer { get; init; }
        public required IServerStreamWriter<MessageEvent> Stream { get; init; }

        /// <summary>Anulat cînd Broker-ul renunță la acest subscriber (livrare eșuată).</summary>
        public CancellationTokenSource Closed { get; } = new();

        // gRPC nu permite scrieri simultane pe același stream, iar worker-ul
        // și redarea istoricului pot scrie în paralel - le serializăm.
        private readonly SemaphoreSlim _writeLock = new(1, 1);
        private readonly HashSet<string> _delivered = new();
        private bool _completed;

        public bool MarkDelivered(string id)
        {
            lock (_delivered)
            {
                return _delivered.Add(id); // false dacă era deja livrat
            }
        }

        public async Task WriteAsync(MessageEvent message)
        {
            await _writeLock.WaitAsync();
            try
            {
                if (_completed)
                {
                    throw new InvalidOperationException("Subscriber-ul s-a deconectat.");
                }

                await Stream.WriteAsync(message).WaitAsync(WriteTimeout);
            }
            finally
            {
                _writeLock.Release();
            }
        }

        /// <summary>
        /// Apelat cînd RPC-ul Subscribe se termină: după asta stream-ul nu
        /// mai poate fi folosit, deci așteptăm să se termine scrierea curentă.
        /// </summary>
        public async Task CompleteAsync()
        {
            await _writeLock.WaitAsync();
            _completed = true;
            _writeLock.Release();
        }
    }

    private readonly ConcurrentDictionary<string, ConcurrentQueue<MessageEvent>> _queues = new();
    private readonly ConcurrentDictionary<string, List<MessageEvent>> _history = new();
    private readonly ConcurrentDictionary<Guid, Subscription> _subscriptions = new();
    private readonly ConcurrentDictionary<string, int> _attempts = new();
    private readonly ConcurrentDictionary<string, byte> _pendingIds = new();
    private readonly ConcurrentDictionary<string, ConcurrentQueue<MessageEvent>> _deadLetters = new();

    // Worker-ul și abonarea unui subscriber nou pe același topic nu rulează în
    // paralel: altfel un subscriber care se abonează exact în timpul livrării
    // ar putea rata un mesaj (nu e încă pe lista worker-ului, dar mesajul nu
    // mai e nici pending ca să-l primească mai tîrziu).
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _topicLocks = new();

    public string StorageDir { get; }

    public GrpcMessageHub(string storageDir)
    {
        StorageDir = storageDir;
        Directory.CreateDirectory(StorageDir);
        RestoreFromDisk();
    }

    private void RestoreFromDisk()
    {
        foreach ((string topic, List<MessageEvent> messages) in GrpcPersistentStore.LoadAll(StorageDir, GrpcPersistentStore.HistoryFolder))
        {
            _history[topic] = messages;
            GetQueue(topic);
        }

        int restored = 0;
        foreach ((string topic, List<MessageEvent> messages) in GrpcPersistentStore.LoadAll(StorageDir, GrpcPersistentStore.PendingFolder))
        {
            ConcurrentQueue<MessageEvent> queue = GetQueue(topic);
            foreach (MessageEvent message in messages)
            {
                _pendingIds[message.Id] = 0;
                queue.Enqueue(message);
                restored++;
            }
        }

        foreach ((string topic, List<MessageEvent> messages) in GrpcPersistentStore.LoadAll(StorageDir, GrpcPersistentStore.DeadLetterFolder))
        {
            _deadLetters[topic] = new ConcurrentQueue<MessageEvent>(messages);
        }

        if (restored > 0)
        {
            Console.WriteLine($"[storage] {restored} mesaj(e) nelivrate restaurate de pe disc.");
        }

        int dead = DeadLetterCount;
        if (dead > 0)
        {
            Console.WriteLine($"[dlq] {dead} mesaj(e) se află în Dead Letter Queue (încărcate de pe disc în memorie):");
            foreach (MessageEvent message in _deadLetters.Values.SelectMany(q => q))
            {
                Console.WriteLine($"[dlq]   [Id={message.Id}] topic '{message.Topic}', de la \"{message.Sender}\": \"{message.Payload}\"");
            }
        }
    }

    /// <summary>
    /// Înregistrează un subscriber și îi trimite întregul istoric al topicului.
    /// </summary>
    public async Task<Guid> SubscribeAsync(Subscription subscription)
    {
        Guid id = Guid.NewGuid();
        GetQueue(subscription.Topic);

        SemaphoreSlim topicLock = GetTopicLock(subscription.Topic);
        await topicLock.WaitAsync();
        try
        {
            _subscriptions[id] = subscription;

            // Doar mesajele deja livrate; cele pending le trimite worker-ul.
            List<MessageEvent> history = GetHistorySnapshot(subscription.Topic)
                .Where(m => !_pendingIds.ContainsKey(m.Id))
                .ToList();

            if (history.Count == 0)
            {
                return id;
            }

            Console.WriteLine($"[history] Trimit {history.Count} mesaj(e) din istoricul topicului '{subscription.Topic}' către \"{subscription.Name}\".");

            foreach (MessageEvent old in history)
            {
                subscription.MarkDelivered(old.Id);
                try
                {
                    await subscription.WriteAsync(old);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[!] Nu s-a putut trimite istoricul către \"{subscription.Name}\" ({subscription.Peer}): {ex.Message}");
                    DropSubscriber(id);
                    break;
                }
            }

            return id;
        }
        finally
        {
            topicLock.Release();
        }
    }

    public SemaphoreSlim GetTopicLock(string topic) =>
        _topicLocks.GetOrAdd(topic, _ => new SemaphoreSlim(1, 1));

    public void Unsubscribe(Guid id) => _subscriptions.TryRemove(id, out _);

    /// <summary>
    /// Scoate un subscriber la care livrarea a eșuat și îi închide RPC-ul.
    /// </summary>
    public void DropSubscriber(Guid id)
    {
        if (_subscriptions.TryRemove(id, out Subscription? subscription))
        {
            subscription.Closed.Cancel();
        }
    }

    /// <summary>
    /// Publicare: mesajul intră în coada topicului (livrare către subscriberii
    /// activi) și în istoricul topicului (pentru subscriberii viitori), atît
    /// în memorie cît și pe disc.
    /// </summary>
    public async Task PublishAsync(MessageEvent message)
    {
        List<MessageEvent> history = _history.GetOrAdd(message.Topic, _ => new List<MessageEvent>());
        lock (history)
        {
            history.Add(message);
        }

        // Întîi pe disc, apoi în coadă: dacă Broker-ul cade imediat după ce
        // Sender-ul a primit confirmarea, mesajul tot e restaurat la repornire.
        await GrpcPersistentStore.AppendHistoryAsync(StorageDir, message);
        await GrpcPersistentStore.SavePendingAsync(StorageDir, message);
        _pendingIds[message.Id] = 0;
        GetQueue(message.Topic).Enqueue(message);

        Console.WriteLine($"[queue:{message.Topic}] Mesaj [Id={message.Id}] adăugat în coadă + istoric (salvat pe disc).");
    }

    private List<MessageEvent> GetHistorySnapshot(string topic)
    {
        if (!_history.TryGetValue(topic, out List<MessageEvent>? history))
        {
            return new List<MessageEvent>();
        }

        lock (history)
        {
            return history.ToList();
        }
    }

    public List<string> Topics => _queues.Keys.ToList();

    public ConcurrentQueue<MessageEvent> GetQueue(string topic) =>
        _queues.GetOrAdd(topic, _ => new ConcurrentQueue<MessageEvent>());

    public List<KeyValuePair<Guid, Subscription>> GetSubscriptions(string topic) =>
        _subscriptions.Where(kv => kv.Value.Topic == topic).ToList();

    public int RegisterAttempt(string messageId) =>
        _attempts.AddOrUpdate(messageId, 1, (_, current) => current + 1);

    public void MarkDelivered(MessageEvent message)
    {
        _attempts.TryRemove(message.Id, out _);
        _pendingIds.TryRemove(message.Id, out _);
        GrpcPersistentStore.DeletePending(StorageDir, message);
    }

    /// <summary>
    /// Mută mesajul în Dead Letter Queue (memorie + disc) și îl scoate din
    /// istoric - nu l-a primit nimeni, deci nu trebuie redat subscriberilor
    /// viitori.
    /// </summary>
    public async Task MoveToDeadLetterAsync(MessageEvent message, string reason)
    {
        _attempts.TryRemove(message.Id, out _);
        _deadLetters.GetOrAdd(message.Topic, _ => new ConcurrentQueue<MessageEvent>()).Enqueue(message);
        await GrpcPersistentStore.SaveDeadLetterAsync(StorageDir, message);

        _pendingIds.TryRemove(message.Id, out _);
        GrpcPersistentStore.DeletePending(StorageDir, message);

        if (_history.TryGetValue(message.Topic, out List<MessageEvent>? history))
        {
            lock (history)
            {
                history.RemoveAll(m => m.Id == message.Id);
            }
        }
        GrpcPersistentStore.DeleteHistory(StorageDir, message);

        Console.WriteLine($"[dlq] Mesaj [Id={message.Id}] mutat în Dead Letter Queue (topic '{message.Topic}') după {MaxDeliveryAttempts} încercări. Motiv: {reason}");
    }

    public int DeadLetterCount => _deadLetters.Values.Sum(q => q.Count);

    public IReadOnlyCollection<MessageEvent> GetDeadLetters(string topic) =>
        _deadLetters.TryGetValue(topic, out ConcurrentQueue<MessageEvent>? queue) ? queue.ToArray() : Array.Empty<MessageEvent>();
}
