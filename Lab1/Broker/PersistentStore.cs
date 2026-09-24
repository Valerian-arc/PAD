using System.Xml.Serialization;
using Common;

namespace Broker;

/// <summary>
/// Stocarea persistentă a Brokerului (metoda persistentă, cerința 1.3.2).
/// Structura pe disc:
///   storage/history/&lt;topic&gt;/&lt;id&gt;.xml     - istoricul complet al topicului
///                                            (nu se șterge; un subscriber nou
///                                            primește tot ce s-a publicat)
///   storage/pending/&lt;topic&gt;/&lt;id&gt;.xml     - mesaje încă nelivrate (coada);
///                                            se șterg după livrare reușită
///   storage/deadletter/&lt;topic&gt;/&lt;id&gt;.xml  - Dead Letter Queue: mesaje care
///                                            nu au putut fi livrate nici după
///                                            MaxDeliveryAttempts încercări
/// Serializarea/deserializarea XML se face asincron, ca să nu blocheze
/// firele care procesează mesajele.
/// </summary>
public static class PersistentStore
{
    private static readonly XmlSerializer Serializer = new(typeof(Message));

    public const string HistoryFolder = "history";
    public const string PendingFolder = "pending";
    public const string DeadLetterFolder = "deadletter";

    public static Task AppendHistoryAsync(string baseDir, Message message) =>
        WriteAsync(Path.Combine(baseDir, HistoryFolder, Sanitize(message.Topic)), message);

    public static Task SavePendingAsync(string baseDir, Message message) =>
        WriteAsync(Path.Combine(baseDir, PendingFolder, Sanitize(message.Topic)), message);

    public static Task SaveDeadLetterAsync(string baseDir, Message message) =>
        WriteAsync(Path.Combine(baseDir, DeadLetterFolder, Sanitize(message.Topic)), message);

    public static void DeletePending(string baseDir, Message message)
    {
        string path = Path.Combine(baseDir, PendingFolder, Sanitize(message.Topic), $"{message.Id}.xml");
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch
        {
            // Ștergerea e doar curățenie - dacă eșuează, nu oprim livrarea.
        }
    }

    /// <summary>
    /// Istoricul unui topic, ordonat cronologic. Folosit pentru a "reda"
    /// conversația unui subscriber care tocmai s-a abonat.
    /// </summary>
    public static List<Message> LoadHistory(string baseDir, string topic) =>
        LoadFolder(Path.Combine(baseDir, HistoryFolder, Sanitize(topic)))
            .OrderBy(m => m.Timestamp)
            .ToList();

    /// <summary>
    /// Mesajele nelivrate rămase din sesiunea anterioară (dacă Brokerul a
    /// fost oprit/a căzut), grupate pe topic.
    /// </summary>
    public static IEnumerable<Message> LoadAllPending(string baseDir)
    {
        string root = Path.Combine(baseDir, PendingFolder);
        if (!Directory.Exists(root))
        {
            yield break;
        }

        foreach (string topicDir in Directory.GetDirectories(root))
        {
            foreach (Message message in LoadFolder(topicDir))
            {
                yield return message;
            }
        }
    }

    public static int CountDeadLetters(string baseDir)
    {
        string root = Path.Combine(baseDir, DeadLetterFolder);
        if (!Directory.Exists(root))
        {
            return 0;
        }

        return Directory.GetDirectories(root).Sum(d => Directory.GetFiles(d, "*.xml").Length);
    }

    private static async Task WriteAsync(string dir, Message message)
    {
        Directory.CreateDirectory(dir);
        string path = Path.Combine(dir, $"{message.Id}.xml");

        await using var memoryStream = new MemoryStream();
        Serializer.Serialize(memoryStream, message);
        await File.WriteAllBytesAsync(path, memoryStream.ToArray());
    }

    private static IEnumerable<Message> LoadFolder(string dir)
    {
        if (!Directory.Exists(dir))
        {
            yield break;
        }

        foreach (string file in Directory.GetFiles(dir, "*.xml"))
        {
            Message? message = null;
            try
            {
                using FileStream fs = File.OpenRead(file);
                message = (Message?)Serializer.Deserialize(fs);
            }
            catch
            {
                // Fișier corupt pe disc - îl ignorăm, Brokerul nu trebuie să cadă.
            }

            if (message != null)
            {
                yield return message;
            }
        }
    }

    private static string Sanitize(string topic)
    {
        if (string.IsNullOrWhiteSpace(topic))
        {
            return "default";
        }

        char[] invalid = Path.GetInvalidFileNameChars();
        return new string(topic.Where(c => !invalid.Contains(c)).ToArray());
    }
}
