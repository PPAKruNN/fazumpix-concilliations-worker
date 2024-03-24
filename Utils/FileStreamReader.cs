
using System.Text.Json;
using ConcilliationWorker.DTOs;

namespace ConcilliationWorker.Utils;

public class FileStreamReader
{
    public async static Task<List<ConcilliationDTO>> StreamReadPaymentChunk(StreamReader streamReader, int chunkSize)
    {
        string? currPaymentStr = "";
        List<ConcilliationDTO> paymentChunk = new();

        while (paymentChunk.Count < chunkSize)
        {
            currPaymentStr = await streamReader.ReadLineAsync();
            if (currPaymentStr is null) break;

            ConcilliationDTO? currConcilliation = JsonSerializer.Deserialize<ConcilliationDTO>(currPaymentStr);
            if (currConcilliation is null) throw new Exception("Invalid message format"); // Stop all function if cannot read into a ConcilliationDTO.

            paymentChunk.Add(currConcilliation);
        }

        return paymentChunk;
    }
}