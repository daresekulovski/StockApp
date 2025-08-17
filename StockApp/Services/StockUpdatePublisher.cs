using Confluent.Kafka;
using System.Text.Json;
using StockApp.Models;
namespace StockApp.Services;

public static class StockUpdatePublisher
{
    public static async Task PublishInitialAsync(CancellationToken ct)
    {
        foreach (var s in MarketState.Stocks)
        {
            var update = new StockUpdate
            {
                Symbol = s,
                Price = MarketState.CurrentPrices[s],
                Timestamp = DateTime.UtcNow
            };
            var msg = new Message<string, string>
            {
                Key = s,
                Value = JsonSerializer.Serialize(update, JsonConfig.Options)
            };
            _ = await KafkaClients.StockUpdateProducer.ProduceAsync(KafkaClients.TopicStockUpdates, msg, ct);
            Console.WriteLine($"[Init] {s} = {update.Price:F2}");
        }
    }
}