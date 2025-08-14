namespace StockApp.Models;

public class StockUpdate
{
        public string Symbol { get; set; }
        public decimal Price { get; set; }
        public DateTime Timestamp { get; set; }
}