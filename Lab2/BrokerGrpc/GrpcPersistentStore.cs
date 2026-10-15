using Google.Protobuf;
using MessageBrokerGrpc;

namespace BrokerGrpc;

/// <summary>
/// Stocarea persistentă a Brokerului gRPC (metoda persistentă, cerința 1.3.2),
/// aceeași structură ca la Partea 1, dar mesajele sînt salvate în formatul
/// binar Protocol Buffers (reprezentarea datelor din Partea 2), nu XML:
///   storage/history/&lt;topic&gt;/&lt;id&gt;.bin     - istoricul complet al topicului
///                                            (nu se șterge)
///   storage/pending/&lt;topic&gt;/&lt;id&gt;.bin     - mesaje încă nelivrate; se șterg
///                                            după livrare, se restaurează la
///                                            repornirea Brokerului
///   storage/deadletter/&lt;topic&gt;/&lt;id&gt;.bin  - Dead Letter Queue
/// Scrierea pe disc se face asincron, ca să nu blocheze firele care
/// procesează RPC-urile.
/// </summary>
public static class GrpcPersistentStore
{
    public const string HistoryFolder = "history";
    public const string PendingFolder = "pending";
    public const string DeadLetterFolder = "deadletter";

    public static Task AppendHistoryAsync(string baseDir, MessageEvent message) =>
        WriteAsync(Path.Combine(baseDir, HistoryFolder, message.Topic), message);

    public static Task SavePendingAsync(string baseDir, MessageEvent message) =>
        WriteAsync(Path.Combine(baseDir, PendingFolder, message.Topic), message);

    public static Task SaveDeadLetterAsync(string baseDir, MessageEvent message) =>
        WriteAsync(Path.Combine(baseDir, DeadLetterFolder, message.Topic), message);

    public static void DeletePending(string baseDir, MessageEvent message) =>
        Delete(Path.Combine(baseDir, PendingFolder, message.Topic, $"{message.Id}.bin"));

    public static void DeleteHistory(string baseDir, MessageEvent message) =>
        Delete(Path.Combine(baseDir, HistoryFolder, message.Topic, $"{message.Id}.bin"));

    private static void Delete(string path)
    {
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
    /// Toate mesajele dintr-o zonă (history/pending), pe topic, în ordine
    /// cronologică (Timestamp e în format ISO 8601, deci se sortează ca text).
    /// </summary>
    public static Dictionary<string, List<MessageEvent>> LoadAll(string baseDir, string folder)
    {
        var result = new Dictionary<string, List<MessageEvent>>();
        string root = Path.Combine(baseDir, folder);
        if (!Directory.Exists(root))
        {
            return result;
        }

        foreach (string topicDir in Directory.GetDirectories(root))
        {
            List<MessageEvent> messages = LoadFolder(topicDir)
                .OrderBy(m => m.Timestamp, StringComparer.Ordinal)
                .ToList();

            if (messages.Count > 0)
            {
                result[Path.GetFileName(topicDir)] = messages;
            }
        }

        return result;
    }

    private static async Task WriteAsync(string dir, MessageEvent message)
    {
        Directory.CreateDirectory(dir);
        string path = Path.Combine(dir, $"{message.Id}.bin");
        await File.WriteAllBytesAsync(path, message.ToByteArray());
    }

    private static IEnumerable<MessageEvent> LoadFolder(string dir)
    {
        foreach (string file in Directory.GetFiles(dir, "*.bin"))
        {
            MessageEvent? message = null;
            try
            {
                message = MessageEvent.Parser.ParseFrom(File.ReadAllBytes(file));
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
}
