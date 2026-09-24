using System.Net.Sockets;
using System.Text;
using System.Text.Json;

namespace Common;

/// <summary>
/// Protocol simplu de transmitere a mesajelor peste TCP:
/// [4 bytes lungime payload][N bytes payload JSON, UTF-8]
///
/// Framing-ul cu lungime este necesar pentru ca TCP este un flux de octeți
/// fără delimitare de mesaje - fără el, mai multe mesaje trimise rapid unul
/// după altul s-ar putea "lipi" la citire.
/// </summary>
public static class MessageProtocol
{
    private const int MaxPayloadSize = 10 * 1024 * 1024; // 10 MB, protecție împotriva mesajelor corupte/enorme

    public static async Task SendMessageAsync(NetworkStream stream, Message message)
    {
        string json = JsonSerializer.Serialize(message);
        byte[] payloadBytes = Encoding.UTF8.GetBytes(json);
        byte[] lengthBytes = BitConverter.GetBytes(payloadBytes.Length);

        await stream.WriteAsync(lengthBytes);
        await stream.WriteAsync(payloadBytes);
        await stream.FlushAsync();
    }

    /// <summary>
    /// Citește un mesaj de pe flux. Întoarce null dacă partenerul a închis
    /// conexiunea (EOF curat). Aruncă excepție dacă datele primite nu
    /// reprezintă un mesaj valid (JSON corupt, lungime aberantă etc.) -
    /// apelantul (Broker/Receiver) trebuie să trateze această excepție și
    /// să NU cadă (cerința 1.1.4: politici de livrare pentru mesaje invalide).
    /// </summary>
    public static async Task<Message?> ReceiveMessageAsync(NetworkStream stream)
    {
        byte[] lengthBuffer = new byte[4];
        int lengthBytesRead = await ReadExactAsync(stream, lengthBuffer, 4);
        if (lengthBytesRead == 0)
        {
            return null; // conexiune închisă curat de partener
        }
        if (lengthBytesRead < 4)
        {
            throw new InvalidDataException("Conexiune închisă în timpul citirii antetului de lungime.");
        }

        int payloadLength = BitConverter.ToInt32(lengthBuffer, 0);
        if (payloadLength <= 0 || payloadLength > MaxPayloadSize)
        {
            throw new InvalidDataException($"Lungime de mesaj invalidă: {payloadLength}.");
        }

        byte[] payloadBuffer = new byte[payloadLength];
        int payloadBytesRead = await ReadExactAsync(stream, payloadBuffer, payloadLength);
        if (payloadBytesRead < payloadLength)
        {
            throw new InvalidDataException("Conexiune închisă în timpul citirii conținutului mesajului.");
        }

        string json = Encoding.UTF8.GetString(payloadBuffer);

        try
        {
            Message? message = JsonSerializer.Deserialize<Message>(json);
            if (message == null)
            {
                throw new InvalidDataException("Mesajul deserializat este null.");
            }
            return message;
        }
        catch (JsonException ex)
        {
            throw new InvalidDataException($"JSON invalid primit: {ex.Message}", ex);
        }
    }

    private static async Task<int> ReadExactAsync(NetworkStream stream, byte[] buffer, int count)
    {
        int totalRead = 0;
        while (totalRead < count)
        {
            int bytesRead = await stream.ReadAsync(buffer.AsMemory(totalRead, count - totalRead));
            if (bytesRead == 0)
            {
                break; // partenerul a închis conexiunea
            }
            totalRead += bytesRead;
        }
        return totalRead;
    }
}
