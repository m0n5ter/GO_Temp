using System.Data.Common;

namespace GO.Workerservice;

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


public class PackageData 
{
    public string df_abstat;
    public string df_empfstat; 
    public string df_linnr;
    public string df_pod;
    public string df_packnr;
    public string df_scandat;
    public string df_scantime;                           
    public string df_scanort;
    public string df_scananlass;
    public string df_errcode;
    public string df_platznr;
    public string df_user;
    public string df_gewicht;
    public string df_kfznr;
    public string df_datschicht;
    public string df_origdb;
    public string df_zieldb; 
    public string df_zieldb1;
    public string df_hub;
    public string df_zieldb2;
    public string df_timestamp;
    public string df_dispoan;
    public string df_manuell;
    public string df_zieldb_auftraggeber;
    public string df_ndl;
    public string df_datauftannahme;
    public string df_lfdnrauftrag;
    public string df_laenge; 
    public string df_breite; 
    public string df_hoehe;

    public string df_kundennr;

    public PackageData(string df_abstat, string df_empfstat, string df_linnr, string df_pod, string df_packnr, 
                       string df_scandat, string df_scantime, string df_scanort, string df_scananlass, string df_errcode, 
                       string df_platznr, string df_user, string df_gewicht, string df_kfznr, string df_datschicht, 
                       string df_origdb, string df_zieldb, string df_zieldb1, string df_hub, string df_zieldb2, 
                       string df_timestamp, string df_dispoan, string df_manuell, string df_zieldb_auftraggeber, 
                       string df_ndl, string df_datauftannahme, string df_lfdnrauftrag, string df_laenge, string df_breite, 
                       string df_hoehe, string df_kundennr)
    {
        this.df_abstat = df_abstat;
        this.df_empfstat = df_empfstat;
        this.df_linnr = df_linnr;
        this.df_pod = df_pod;
        this.df_packnr = df_packnr;
        this.df_scandat = df_scandat;
        this.df_scantime = df_scantime;
        this.df_scanort = df_scanort;
        this.df_scananlass = df_scananlass;
        this.df_errcode = df_errcode;
        this.df_platznr = df_platznr;
        this.df_user = df_user;
        this.df_gewicht = df_gewicht;
        this.df_kfznr = df_kfznr;
        this.df_datschicht = df_datschicht;
        this.df_origdb = df_origdb;
        this.df_zieldb = df_zieldb;
        this.df_zieldb1 = df_zieldb1;
        this.df_hub = df_hub;
        this.df_zieldb2 = df_zieldb2;
        this.df_timestamp = df_timestamp;
        this.df_dispoan = df_dispoan;
        this.df_manuell = df_manuell;
        this.df_zieldb_auftraggeber = df_zieldb_auftraggeber;
        this.df_ndl = df_ndl;
        this.df_datauftannahme = df_datauftannahme;
        this.df_lfdnrauftrag = df_lfdnrauftrag;
        this.df_laenge = df_laenge;
        this.df_breite = df_breite;
        this.df_hoehe = df_hoehe;
        this.df_kundennr = df_kundennr;
    }
}