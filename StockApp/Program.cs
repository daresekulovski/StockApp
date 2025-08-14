using StockApp.Services;

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

await StockUpdatePublisher.PublishInitialAsync(cts.Token);

var processorTask = OrderProcessor.RunAsync(cts.Token);
var adjusterTask  = PriceAdjuster.RunAsync(TimeSpan.FromSeconds(5), cts.Token);
//var driftTask     = StockUpdatePublisher.RunRandomDriftAsync(TimeSpan.FromSeconds(10), cts.Token);

int simulatedTraders = 10000;
double actProbability = 0.02;
int maxOrdersPerTick = 750;
var pool = new TraderPool(simulatedTraders, actProbability, maxOrdersPerTick);
var traderPoolTask = TraderPool.RunSingleConsumerAsync(pool, cts.Token);

Console.WriteLine($"Running with {simulatedTraders:N0} simulated traders…  Press Ctrl+C to stop.");

await Task.WhenAll(processorTask, adjusterTask, /*driftTask,*/ traderPoolTask);

KafkaClients.FlushAll(TimeSpan.FromSeconds(5));
Console.WriteLine("Shutdown complete.");