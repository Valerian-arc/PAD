using System.Collections.Concurrent;
using Microsoft.Extensions.Hosting;
using MessageBrokerGrpc;

namespace BrokerGrpc;

/// <summary>
/// "Work job" / Cron job: la fiecare secundă livrează mesajele din coada
/// fiecărui topic către subscriberii activi ai topicului respectiv. Topicurile
/// sînt procesate concurent, fiecare pe Task-ul lui.
///
/// Politica de livrare (cerința 1.1.4 + Dead Letter Queue):
///  - mesajul e scris direct pe stream-ul gRPC al fiecărui subscriber; dacă
///    scrierea eșuează (client deconectat, timeout), subscriber-ul e scos;
///  - dacă livrarea către toți subscriberii eșuează, mesajul revine în coadă;
///  - după 3 încercări eșuate, mesajul e mutat în Dead Letter Queue;
///  - un mesaj pentru care nu există niciun subscriber NU consumă încercări -
///    rămîne stocat (și pe disc) pînă apare unul.
/// </summary>
public sealed class GrpcDeliveryWorker : BackgroundService
{
    private readonly GrpcMessageHub _hub;
    private readonly TimeSpan _interval = TimeSpan.FromSeconds(1);

    public GrpcDeliveryWorker(GrpcMessageHub hub)
    {
        _hub = hub;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.WhenAll(_hub.Topics.Select(DeliverTopicAsync));
            await Task.Delay(_interval, stoppingToken);
        }
    }

    private async Task DeliverTopicAsync(string topic)
    {
        ConcurrentQueue<MessageEvent> queue = _hub.GetQueue(topic);
        if (queue.IsEmpty)
        {
            return;
        }

        SemaphoreSlim topicLock = _hub.GetTopicLock(topic);
        await topicLock.WaitAsync();
        try
        {
            await DeliverQueuedAsync(topic, queue);
        }
        finally
        {
            topicLock.Release();
        }
    }

    private async Task DeliverQueuedAsync(string topic, ConcurrentQueue<MessageEvent> queue)
    {
        int pending = queue.Count;

        for (int i = 0; i < pending; i++)
        {
            // Lista se reia la fiecare mesaj, ca subscriberii scoși la mesajul
            // anterior să nu mai fie încercați.
            List<KeyValuePair<Guid, GrpcMessageHub.Subscription>> subscriptions = _hub.GetSubscriptions(topic);
            if (subscriptions.Count == 0)
            {
                return; // fără subscriberi: mesajele rămîn în coadă
            }

            if (!queue.TryDequeue(out MessageEvent? message))
            {
                break;
            }

            int attempt = _hub.RegisterAttempt(message.Id);
            int delivered = 0;
            string lastError = "necunoscut";

            foreach ((Guid id, GrpcMessageHub.Subscription subscription) in subscriptions)
            {
                if (_hub.IsDemoPoison(message))
                {
                    // Eșec cauzat de mesaj, nu de subscriber - subscriber-ul rămîne
                    // conectat, iar mesajul e reîncercat la runda următoare.
                    lastError = "mesaj otrăvit (simulare --demo-dlq)";
                    Console.WriteLine($"[!] Livrare eșuată [Id={message.Id}] către \"{subscription.Name}\" ({subscription.Peer}): {lastError}");
                    continue;
                }

                if (!subscription.MarkDelivered(message.Id))
                {
                    delivered++; // primit deja - considerăm livrat
                    continue;
                }

                try
                {
                    await subscription.WriteAsync(message);
                    delivered++;
                }
                catch (Exception ex)
                {
                    lastError = ex.Message;
                    Console.WriteLine($"[!] Livrare eșuată [Id={message.Id}] către \"{subscription.Name}\" ({subscription.Peer}): {ex.Message}");
                    _hub.DropSubscriber(id);
                }
            }

            if (delivered > 0)
            {
                _hub.MarkDelivered(message);
                Console.WriteLine($"[worker] Mesaj [Id={message.Id}] livrat la {delivered} subscriber(i) pe topicul '{topic}'.");
                continue;
            }

            if (attempt >= GrpcMessageHub.MaxDeliveryAttempts)
            {
                await _hub.MoveToDeadLetterAsync(message, lastError);
            }
            else
            {
                Console.WriteLine($"[retry] Mesaj [Id={message.Id}] - încercarea {attempt}/{GrpcMessageHub.MaxDeliveryAttempts} eșuată, revine în coadă.");
                queue.Enqueue(message);
            }
        }
    }
}
