using System.Text.Json.Serialization;

namespace ConcilliationWorker.DTOs;

// {"id": number, "status": string}
public class ConcilliationDTO
{
    [JsonPropertyName("id")]
    public required int Id { get; set; }
    [JsonPropertyName("status")]
    public required string Status { get; set; }
}