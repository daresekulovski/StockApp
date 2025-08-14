using Confluent.Kafka;
using System.Text.Json;
using StockApp.Models;
namespace StockApp.Services;

public static class PriceAdjuster
{
    private const decimal AdjustmentPerShare = 0.01m;
    private const decimal MaxPercentChange = 0.05m;

    public static Task RunAsync(TimeSpan interval, CancellationToken ct) =>
        Task.Run(async () =>
        {
            while (!ct.IsCancellationRequested)
            {
                foreach (var stock in MarketState.Stocks)
                {
                    var buy = MarketState.PendingBuyVolume.GetValueOrDefault(stock, 0);
                    var sell = MarketState.PendingSellVolume.GetValueOrDefault(stock, 0);
                    var net = buy - sell;
                    if (net == 0) continue;

                    var current = MarketState.CurrentPrices[stock];
                    var rawAdj = (decimal)net * AdjustmentPerShare;
                    var maxAdj = current * MaxPercentChange;
                    var clamped = Math.Clamp(rawAdj, -maxAdj, maxAdj);

                    var newPrice = Math.Max(0.01m, current + clamped);
                    MarketState.CurrentPrices[stock] = newPrice;

                    var update = new StockUpdate { Symbol = stock, Price = newPrice, Timestamp = DateTime.UtcNow };
                    KafkaClients.StockUpdateProducer.Produce(
                        KafkaClients.TopicStockUpdates,
                        new Message<string, string> { Key = stock, Value = JsonSerializer.Serialize(update, JsonConfig.Options) });

                    Console.WriteLine($"[Adjust] {stock} net {net,6} | {current,8:F2} -> {newPrice,8:F2}");
                }

                await Task.Delay(interval, ct);
            }
        }, ct);
}