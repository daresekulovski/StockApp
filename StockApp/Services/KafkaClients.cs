using Confluent.Kafka;
namespace StockApp.Services;

public static class KafkaClients
{
    public const string Bootstrap = "localhost:9092";
    public const string TopicStockUpdates = "stock-updates";
    public const string TopicOrders = "orders";

    
    public static readonly IProducer<string, string> StockUpdateProducer;
    public static readonly IProducer<string, string> OrderProducer;

    static KafkaClients()
    {
        var common = new ProducerConfig
        {
            BootstrapServers = Bootstrap
        };

        StockUpdateProducer = new ProducerBuilder<string, string>(common).Build();
        OrderProducer = new ProducerBuilder<string, string>(common).Build();
    }

    public static void FlushAll(TimeSpan timeout)
    {
        try { StockUpdateProducer.Flush(timeout); } catch { }
        try { OrderProducer.Flush(timeout); } catch { }
    }
}