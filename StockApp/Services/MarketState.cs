using System.Collections.Concurrent;
using StockApp.Models;
namespace StockApp.Services;

public static class MarketState
{
    public static readonly string[] Stocks = { "AAPL", "GOOG", "NVDA", "META", "AMD" };

    public static readonly ConcurrentDictionary<string, decimal> CurrentPrices = new()
    {
        ["AAPL"] = 150m, ["GOOG"] = 2500m, ["NVDA"] = 700m, ["META"] = 300m, ["AMD"] = 100m
    };
    
    public static readonly ConcurrentDictionary<string, ConcurrentQueue<Order>> BuyQueues = new();
    public static readonly ConcurrentDictionary<string, ConcurrentQueue<Order>> SellQueues = new();
    
    public static readonly ConcurrentDictionary<string, long> PendingBuyVolume = new();
    public static readonly ConcurrentDictionary<string, long> PendingSellVolume = new();

    static MarketState()
    {
        foreach (var s in Stocks)
        {
            BuyQueues.TryAdd(s, new ConcurrentQueue<Order>());
            SellQueues.TryAdd(s, new ConcurrentQueue<Order>());
            PendingBuyVolume.TryAdd(s, 0);
            PendingSellVolume.TryAdd(s, 0);
        }
    }
}