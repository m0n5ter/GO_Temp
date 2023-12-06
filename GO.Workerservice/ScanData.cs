using System.Data.Common;

namespace GO.Workerservice;

public class ScanData2
{
    private string DF_ABSTAT;
    private string DF_EMPFSTAT;
    private string DF_LINNR;
    private string DF_POD;
    private int DF_PACKNR;
    private DateTime DF_SCANDAT;
    private TimeSpan DF_SCANTIME;

    public ScanData2(DbDataReader reader)
    {
        DF_ABSTAT = (string)reader[nameof(DF_ABSTAT)];
        DF_EMPFSTAT = (string)reader[nameof(DF_EMPFSTAT)];
        DF_LINNR = (string)reader[nameof(DF_LINNR)];
        DF_POD = (string)reader[nameof(DF_POD)];
        DF_PACKNR = (int)reader[nameof(DF_PACKNR)];
        DF_SCANDAT = (DateTime)reader[nameof(DF_SCANDAT)];
        DF_SCANTIME = (TimeSpan)reader[nameof(DF_SCANTIME)];
    }
}

public class ScanData 
{
    private string df_abstat;
    private string df_empfstat; 
    private string df_linnr;
    private string df_pod;
    private string df_packnr;
    private string df_scandat;
    private string df_scantime;                           
    private string df_scanort;
    private string df_scananlass;
    private string df_errcode;
    private string df_platznr;
    private string df_user;
    private string df_gewicht;
    private string df_kfznr;
    private string df_datschicht;
    private string df_origdb;
    private string df_zieldb; 
    private string df_zieldb1;
    private string df_hub;
    private string df_zieldb2;
    private string df_timestamp;
    private string df_dispoan;
    private string df_manuell;
    private string df_zieldb_auftraggeber;
    private string df_ndl;
    private string df_datauftannahme;
    private string df_lfdnrauftrag;
    private string df_laenge; 
    private string df_breite; 
    private string df_hoehe;

    public ScanData(string df_abstat, string df_empfstat, string df_linnr, string df_pod, string df_packnr, 
                       string df_scandat, string df_scantime, string df_scanort, string df_scananlass, string df_errcode, 
                       string df_platznr, string df_user, string df_gewicht, string df_kfznr, string df_datschicht, 
                       string df_origdb, string df_zieldb, string df_zieldb1, string df_hub, string df_zieldb2, 
                       string df_timestamp, string df_dispoan, string df_manuell, string df_zieldb_auftraggeber, 
                       string df_ndl, string df_datauftannahme, string df_lfdnrauftrag, string df_laenge, string df_breite, 
                       string df_hoehe)
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
    }
}