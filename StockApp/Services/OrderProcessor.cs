using Confluent.Kafka;
using System.Collections.Concurrent;
using System.Text.Json;
using StockApp.Models;
namespace StockApp.Services;

public class OrderProcessor
{
    public static Task RunAsync(CancellationToken ct) =>
        Task.Run(() =>
        {
            var cfg = new ConsumerConfig
            {
                BootstrapServers = KafkaClients.Bootstrap,
                GroupId = "order-processor-group",
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnableAutoCommit = true
            };

            using var consumer = new ConsumerBuilder<string, string>(cfg).Build();
            consumer.Subscribe(KafkaClients.TopicOrders);

            try
            {
                while (!ct.IsCancellationRequested)
                {
                    var cr = consumer.Consume(TimeSpan.FromMilliseconds(100));
                    if (cr == null || cr.Message == null) continue;

                    var order = JsonSerializer.Deserialize<Order>(cr.Message.Value, JsonConfig.Options);
                    if (order is null) continue;

                    var symbol = order.Symbol;
                    var buyQ = MarketState.BuyQueues.GetOrAdd(symbol, _ => new ConcurrentQueue<Order>());
                    var sellQ = MarketState.SellQueues.GetOrAdd(symbol, _ => new ConcurrentQueue<Order>());

                    if (order.Type.Equals("buy", StringComparison.OrdinalIgnoreCase))
                    {
                        buyQ.Enqueue(order);
                        AddVolume(MarketState.PendingBuyVolume, symbol, order.Quantity);
                    }
                    else
                    {
                        sellQ.Enqueue(order);
                        AddVolume(MarketState.PendingSellVolume, symbol, order.Quantity);
                    }
                    while (!buyQ.IsEmpty && !sellQ.IsEmpty)
                    {
                        if (!buyQ.TryDequeue(out var buy) || !sellQ.TryDequeue(out var sell))
                            break;
                        
                        AddVolume(MarketState.PendingBuyVolume, symbol, -buy.Quantity);
                        AddVolume(MarketState.PendingSellVolume, symbol, -sell.Quantity);

                        var qty = Math.Min(buy.Quantity, sell.Quantity);

                        //Console.WriteLine($"[MATCH] {symbol} {qty} @ {tradePrice:F2} | Buyer {buy.TraderId} / Seller {sell.TraderId}");
                        
                        if (buy.Quantity > qty)
                        {
                            buy.Quantity -= qty;
                            buyQ.Enqueue(buy);
                            AddVolume(MarketState.PendingBuyVolume, symbol, buy.Quantity);
                        }

                        if (sell.Quantity > qty)
                        {
                            sell.Quantity -= qty;
                            sellQ.Enqueue(sell);
                            AddVolume(MarketState.PendingSellVolume, symbol, sell.Quantity);
                        }
                    }
                }
            }
            catch (OperationCanceledException) { }
            finally
            {
                consumer.Close();
            }
        }, ct);

    private static void AddVolume(ConcurrentDictionary<string, long> dict, string symbol, long delta)
        => dict.AddOrUpdate(symbol, delta, (_, prev) => prev + delta);
}