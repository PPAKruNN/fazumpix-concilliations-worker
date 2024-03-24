
using System.Net.NetworkInformation;
using System.Text.Json;
using ConcilliationWorker.DTOs;

namespace ConcilliationWorker.Utils;

public class PaymentUtils
{

    public async static Task SortDiffAndMissing(List<ConcilliationDTO> pspPayments, List<ConcilliationDTO> databasePayments, FileHelper fileHelper)
    {

        List<int> dbPaymentIds = databasePayments.Select(x => x.Id).ToList();

        using StreamWriter missingOut = new(fileHelper.FileToDatabaseFilename, append: true);
        using StreamWriter okOut = new(fileHelper.OkFilename, append: true);
        using StreamWriter diffOut = new(fileHelper.DifferentStatusFilename, append: true);

        for (int i = 0; i < pspPayments.Count; i++)
        {
            ConcilliationDTO pspPayment = pspPayments[i];

            var dbIndex = dbPaymentIds.BinarySearch(pspPayment.Id);

            // If not found, write to missing (fileToDatabase).
            if (dbIndex < 0)
            {
                await missingOut.WriteLineAsync(JsonSerializer.Serialize(pspPayment));
                continue;
            }

            var currDb = databasePayments[dbIndex];

            // If found, write to ok.
            if (pspPayment.Status == currDb.Status)
            {
                await okOut.WriteLineAsync(JsonSerializer.Serialize(pspPayment));
                continue;
            }

            // If not found, write to diff (differentStatus).
            if (pspPayment.Status != currDb.Status)
            {
                await diffOut.WriteLineAsync(JsonSerializer.Serialize(pspPayment));
                continue;
            }

        };

        diffOut.Close();
        okOut.Close();
        missingOut.Close();
    }

    public async static Task SortDatabaseOnly(List<ConcilliationDTO> dbChunk, FileHelper fileHelper)
    {
        using StreamReader okFile = new(fileHelper.OkFilename);
        using StreamReader diffFile = new(fileHelper.DifferentStatusFilename);
        using StreamWriter dbOnly = new(fileHelper.DatabaseToFileFilename, append: true);

        List<int> ids = dbChunk.Select(x => x.Id).ToList();
        if (ids.Count == 0) return;

        // Iterate over okFile. 
        while (okFile.Peek() > -1)
        {
            var paymentChunk = await FileStreamReader.StreamReadPaymentChunk(okFile, 1024);
            paymentChunk.ForEach((currPayment) =>
            {
                int foundIndex = ids.BinarySearch(currPayment.Id);
                if (foundIndex >= 0) ids[foundIndex] = -1;
            });
        }

        // Iterate over diffFile. 
        while (diffFile.Peek() > -1)
        {
            var paymentChunk = await FileStreamReader.StreamReadPaymentChunk(diffFile, 1024);
            paymentChunk.ForEach((currPayment) =>
            {
                int foundIndex = ids.BinarySearch(currPayment.Id);
                if (foundIndex >= 0) ids[foundIndex] = -1;
            });
        }

        // Iterate over ids to write to dbOnly.
        for (int i = 0; i < ids.Count; i++)
        {
            if (ids[i] == -1) continue;
            dbOnly.WriteLine(JsonSerializer.Serialize(dbChunk[i]));
        }

        okFile.Close();
        diffFile.Close();
        dbOnly.Close();
    }

    private async static Task JsonWriteArray(string arrayname, Utf8JsonWriter target, StreamReader from)
    {
        target.WriteStartArray(arrayname);
        while (from.Peek() > -1)
        {
            string? currString = await from.ReadLineAsync();
            if (currString is null) break;

            ConcilliationDTO? curr = JsonSerializer.Deserialize<ConcilliationDTO>(currString);
            if (curr is null) throw new Exception("Invalid message format");

            target.WriteStartObject();
            target.WriteNumber("id", curr.Id);
            target.WriteString("status", curr.Status);
            target.WriteEndObject();
        }
        target.WriteEndArray();
    }

    public async static Task Assemble(FileHelper fileHelper)
    {
        // Sorted files 
        using StreamReader missingFile = new(fileHelper.FileToDatabaseFilename);
        using StreamReader dbOnlyFile = new(fileHelper.DatabaseToFileFilename);
        using StreamReader diffFile = new(fileHelper.DifferentStatusFilename);

        // Output final file.
        using StreamWriter assembleStream = new(fileHelper.AssemblyFilename);
        using Utf8JsonWriter assembleFile = new(assembleStream.BaseStream);

        assembleFile.WriteStartObject();

        await JsonWriteArray("fileToDatabase", assembleFile, missingFile);
        await JsonWriteArray("databaseToFile", assembleFile, dbOnlyFile);
        await JsonWriteArray("differentStatus", assembleFile, diffFile);

        assembleFile.WriteEndObject();

        // Cleanup
        assembleFile.Dispose();
        assembleStream.Dispose();
        assembleStream.Close();

        missingFile.Close();
        dbOnlyFile.Close();
        diffFile.Close();
    }

}