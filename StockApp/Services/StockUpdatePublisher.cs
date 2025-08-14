using Confluent.Kafka;
using System.Text.Json;
using StockApp.Models;
namespace StockApp.Services;

public static class StockUpdatePublisher
{
   private static readonly Random _rand = new();
   
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
    
    public static Task RunRandomDriftAsync(TimeSpan interval, CancellationToken ct) =>
        Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                foreach (var s in MarketState.Stocks)
                {
                    var price = MarketState.CurrentPrices[s];
                    var delta = price * (decimal)((_rand.NextDouble() - 0.5) * 0.004);
                    var newPrice = Math.Max(0.01m, price + delta);
                    if (newPrice != price)
                    {
                        MarketState.CurrentPrices[s] = newPrice;
                        var update = new StockUpdate { Symbol = s, Price = newPrice, Timestamp = DateTime.UtcNow };
                        KafkaClients.StockUpdateProducer.Produce(
                            KafkaClients.TopicStockUpdates,
                            new Message<string, string> { Key = s, Value = JsonSerializer.Serialize(update, JsonConfig.Options) });
                        Console.WriteLine($"[Drift] {s} -> {newPrice:F2}");
                    }
                }
                await Task.Delay(interval, ct);
            }
        }, ct); 
}