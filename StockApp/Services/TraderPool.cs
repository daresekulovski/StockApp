using Confluent.Kafka;
using System.Text.Json;
using StockApp.Models;
namespace StockApp.Services;

public sealed class TraderPool
{
     private readonly List<Guid> _traders;
    private readonly Random _rand = new();
    private readonly double _actProbability;
    private readonly int _maxOrdersPerTick;

    public TraderPool(int traderCount, double actProbability = 0.02, int maxOrdersPerTick = 500)
    {
        if (traderCount <= 0) throw new ArgumentOutOfRangeException(nameof(traderCount));
        if (actProbability < 0 || actProbability > 1) throw new ArgumentOutOfRangeException(nameof(actProbability));
        if (maxOrdersPerTick <= 0) throw new ArgumentOutOfRangeException(nameof(maxOrdersPerTick));

        _traders = Enumerable.Range(0, traderCount).Select(_ => Guid.NewGuid()).ToList();
        _actProbability = actProbability;
        _maxOrdersPerTick = maxOrdersPerTick;
    }

    public void ProcessStockUpdate(StockUpdate update)
    {
        int actions = 0;
        int sampleSize = Math.Min(_traders.Count, _maxOrdersPerTick * 10);
        for (int i = 0; i < sampleSize && actions < _maxOrdersPerTick; i++)
        {
            var traderId = _traders[_rand.Next(_traders.Count)];
            if (_rand.NextDouble() <= _actProbability)
            {
                var order = new Order
                {
                    Symbol = update.Symbol,
                    Type = _rand.NextDouble() > 0.5 ? "buy" : "sell",
                    Quantity = _rand.Next(1, 100),
                    Price = update.Price,
                    TraderId = traderId
                };

                KafkaClients.OrderProducer.Produce(
                    KafkaClients.TopicOrders,
                    new Message<string, string>
                    {
                        Key = order.Symbol,
                        Value = JsonSerializer.Serialize(order, JsonConfig.Options)
                    });

                actions++;
            }
        }
        if (actions > 0)
            Console.WriteLine($"[TraderPool] {update.Symbol} tick -> {actions} orders");
    }

    public static Task RunSingleConsumerAsync(TraderPool pool, CancellationToken ct) =>
        Task.Run(() =>
        {
            var cfg = new ConsumerConfig
            {
                BootstrapServers = KafkaClients.Bootstrap,
                GroupId = "trader-pool-consumer",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = true
            };
            using var consumer = new ConsumerBuilder<string, string>(cfg).Build();
            consumer.Subscribe(KafkaClients.TopicStockUpdates);

            try
            {
                while (!ct.IsCancellationRequested)
                {
                    var cr = consumer.Consume(TimeSpan.FromMilliseconds(100));
                    if (cr == null || cr.Message == null) continue;

                    var update = JsonSerializer.Deserialize<StockUpdate>(cr.Message.Value, JsonConfig.Options);
                    if (update is null) continue;

                    pool.ProcessStockUpdate(update);
                }
            }
            catch (OperationCanceledException) { }
            finally
            {
                consumer.Close();
            }
        }, ct);
}