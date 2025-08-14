namespace StockApp.Models;

public class Order
{
    public string Symbol { get; set; }
    public string Type { get; set; }
    public int Quantity { get; set; }
    public decimal Price { get; set; }
    public Guid TraderId { get; set; }
}