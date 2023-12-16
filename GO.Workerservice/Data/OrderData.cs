using System.Data.Common;

namespace GO.Workerservice.Data;

public class OrderData(DbDataReader reader)
{
    public string DF_NDL { get; init; } = (string) reader[nameof(DF_NDL)];
    public DateTime DF_DATAUFTANNAHME { get; init; } = (DateTime) reader[nameof(DF_DATAUFTANNAHME)];
    public int DF_LFDNRAUFTRAG { get; init; } = (int) reader[nameof(DF_LFDNRAUFTRAG)];
    public string DF_POD { get; init; } = (string) reader[nameof(DF_POD)];
    public string DF_HUB { get; init; } = (string) reader[nameof(DF_HUB)];
    public string zieldb { get; init; } = (string)reader[nameof(zieldb)];
    public string zieldb1 { get; init; } = (string)reader[nameof(zieldb1)];
    public int DF_KUNDENNR { get; init; } = (int) reader[nameof(DF_KUNDENNR)];
}