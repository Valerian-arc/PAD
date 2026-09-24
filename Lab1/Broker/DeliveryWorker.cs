using Common;

namespace Broker;

/// <summary>
/// "Work job" / Cron job: la interval fix, parcurge coada FIECĂRUI topic și
/// livrează mesajele către subscriberii activi ai acelui topic.
///
/// Politica de livrare (cerința 1.1.4 + bonus Dead Letter Queue):
///  - dacă livrarea către toți subscriberii eșuează, mesajul e pus înapoi în
///    coadă și se reîncearcă;
///  - după BrokerHub.MaxDeliveryAttempts (3) încercări eșuate, mesajul e
///    mutat în Dead Letter Queue și nu mai blochează coada.
/// Un mesaj pentru care nu există încă niciun subscriber NU consumă încercări -
/// rămîne pur și simplu stocat pînă apare un subscriber.
/// </summary>
public sealed class DeliveryWorker
{
    private readonly BrokerHub _hub;
    private readonly TimeSpan _interval;

    public DeliveryWorker(BrokerHub hub, TimeSpan interval)
    {
        _hub = hub;
        _interval = interval;
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            // Fiecare topic e procesat concurent, pe Task-ul lui.
            await Task.WhenAll(_hub.Topics.Select(DeliverTopicAsync));
            await Task.Delay(_interval, cancellationToken);
        }
    }

    private async Task DeliverTopicAsync(string topic)
    {
        var queue = _hub.GetQueueForTopic(topic);
        if (queue.IsEmpty)
        {
            return;
        }

        List<KeyValuePair<Guid, SubscriberConnection>> subscribers = _hub.GetSubscribers(topic);
        if (subscribers.Count == 0)
        {
            // Niciun subscriber conectat: mesajele rămîn în coadă, fără a
            // consuma încercări de livrare.
            return;
        }

        int pending = queue.Count;

        for (int i = 0; i < pending; i++)
        {
            if (!queue.TryDequeue(out Message? message))
            {
                break;
            }

            message.DeliveryAttempts++;

            int delivered = 0;
            var deadSubscribers = new List<Guid>();
            string lastError = "necunoscut";

            foreach ((Guid id, SubscriberConnection subscriber) in subscribers)
            {
                if (!subscriber.MarkDelivered(message.Id))
                {
                    delivered++; // primit deja prin istoric - considerăm livrat
                    continue;
                }

                try
                {
                    await MessageProtocol.SendMessageAsync(subscriber.Stream, message);
                    delivered++;
                }
                catch (Exception ex)
                {
                    lastError = ex.Message;
                    Console.WriteLine($"[!] Livrare eșuată [Id={message.Id}] către {subscriber.Endpoint}: {ex.Message}");
                    deadSubscribers.Add(id);
                }
            }

            foreach (Guid id in deadSubscribers)
            {
                _hub.RemoveSubscriber(id);
            }

            if (delivered > 0)
            {
                PersistentStore.DeletePending(_hub.StorageDir, message);
                Console.WriteLine($"[worker] Mesaj [Id={message.Id}] livrat la {delivered} subscriber(i) pe topicul '{topic}'.");
                continue;
            }

            // Nicio livrare reușită la această încercare.
            if (message.DeliveryAttempts >= BrokerHub.MaxDeliveryAttempts)
            {
                await _hub.MoveToDeadLetterAsync(message, lastError);
            }
            else
            {
                Console.WriteLine($"[retry] Mesaj [Id={message.Id}] - încercarea {message.DeliveryAttempts}/{BrokerHub.MaxDeliveryAttempts} eșuată, revine în coadă.");
                queue.Enqueue(message);
            }
        }
    }
}
