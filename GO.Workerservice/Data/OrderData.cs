using System.Data.Common;

namespace GO.Workerservice.Data;

public class OrderData
{
    public string DF_NDL { get; init; }
    public DateTime DF_DATAUFTANNAHME { get; init; }
    public int DF_LFDNRAUFTRAG { get; init; }
    public string DF_POD { get; init; }
    public string DF_HUB { get; init; }
    public string zieldb { get; init; }
    public string zieldb1 { get; init; }
    public int DF_KUNDENNR { get; init; }

    public OrderData(DbDataReader reader)
    {
        DF_NDL = (string) reader[nameof(DF_NDL)];
        DF_DATAUFTANNAHME = (DateTime) reader[nameof(DF_DATAUFTANNAHME)];
        DF_LFDNRAUFTRAG = (int) reader[nameof(DF_LFDNRAUFTRAG)];
        DF_POD = (string) reader[nameof(DF_POD)];
        DF_HUB = (string) reader[nameof(DF_HUB)];
        DF_KUNDENNR = (int) reader[nameof(DF_KUNDENNR)];
        zieldb = (string)reader[nameof(zieldb)];
        zieldb1 = (string)reader[nameof(zieldb1)];
    }
}