using Dapper;
using Npgsql;
using ConcilliationWorker.DTOs;

namespace ConcilliationWorker.Database;

public class DatabaseHandler
{

    public static async Task<List<ConcilliationDTO>> FindPayments(NpgsqlConnection conn, List<ConcilliationDTO> toFind, int PaymentProviderId, DateTime date)
    {
        List<ConcilliationDTO> found =
        (List<ConcilliationDTO>)await conn.QueryAsync<ConcilliationDTO>(@"SELECT ""Payment"".""Id"", ""Payment"".""Status"" 
                                                                        FROM ""Payment""
                                                                        Join ""PaymentProviderAccount"" AS ""origin""
                                                                        ON ""origin"".""Id"" = ""Payment"".""OriginPaymentProviderAccountId""
                                                                        Join ""PaymentProviderAccount"" AS ""destination""
                                                                        ON ""destination"".""Id"" = ""Payment"".""DestinationPaymentProviderAccountId""
                                                                        WHERE DATE_TRUNC('day', ""Payment"".""CreatedAt"") = @Date
                                                                        AND ""Payment"".""Id"" = ANY(@Ids)
                                                                        AND(""origin"".""PaymentProviderId"" = @ProviderId
                                                                        OR ""destination"".""PaymentProviderId"" = @ProviderId)",
                                                                        new { Ids = toFind.Select(x => x.Id).ToArray(), Date = date, ProviderId = PaymentProviderId });
        return found;
    }

    public static async Task<List<ConcilliationDTO>> QueryPayments(NpgsqlConnection conn, int chunkSize, int iteration, int PaymentProviderId, DateTime date)
    {

        List<ConcilliationDTO> found =
        (List<ConcilliationDTO>)await conn.QueryAsync<ConcilliationDTO>(@$"SELECT ""Payment"".""Id"", ""Payment"".""Status"" 
                                                                        FROM ""Payment""
                                                                        Join ""PaymentProviderAccount"" AS ""origin""
                                                                        ON ""origin"".""Id"" = ""Payment"".""OriginPaymentProviderAccountId""
                                                                        Join ""PaymentProviderAccount"" AS ""destination""
                                                                        ON ""destination"".""Id"" = ""Payment"".""DestinationPaymentProviderAccountId""
                                                                        WHERE DATE_TRUNC('day', ""Payment"".""CreatedAt"") = @Date
                                                                        AND(""origin"".""PaymentProviderId"" = @ProviderId
                                                                        OR ""destination"".""PaymentProviderId"" = @ProviderId)
                                                                        ORDER BY ""Payment"".""Id"" ASC
                                                                        LIMIT @Limit
                                                                        OFFSET @Skip",
                                                                        new { Date = date, ProviderId = PaymentProviderId, Limit = chunkSize, Skip = iteration * chunkSize });
        return found;
    }
}