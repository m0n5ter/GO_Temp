namespace GO.Workerservice;

using Microsoft.Extensions.Logging;
using System.Data.Odbc;

public class DatabaseService
{
    private readonly DatabaseConfiguration _databaseConfiguration;
    private readonly ILogger<DatabaseService> _logger;

    public DatabaseService(Configuration configuration, ILogger<DatabaseService> logger)
    {
        _databaseConfiguration = configuration.DatabaseConfiguration;
        _logger = logger;
    }

    private async Task<T> Execute<T>(Func<OdbcConnection, Task<T>> task)
    {
        await using var connection = new OdbcConnection();
        connection.ConnectionString = _databaseConfiguration.ConnectionString;
        await connection.OpenAsync();

        try
        {
            return await task(connection);
        }
        finally
        {
            await connection.CloseAsync();
        }
    }

    private async Task Execute(Func<OdbcConnection, Task> action) => await Execute(new Func<OdbcConnection, Task<object?>>(async c =>
    {
        await action(c);
        return null;
    }));

    public Task<PackageData?> GetOrderAsync(string freightLetterNumber) => Execute(async connection =>
    {
        var cmd = connection.CreateCommand();

        cmd.CommandText = @"SELECT FIRST *,
                            (select if df_kz_go>0 then 'go'+lower(df_hstat) else null endif from dba.tb_stationen
                            where df_stat=df_abstat) zieldb1,
                            (select if df_kz_go>0 then 'go'+lower(df_hstat) else null endif from dba.tb_stationen
                            where df_stat=df_empfstat) zieldb,
                            current database origdb
                            FROM DBA.TB_AUFTRAG
                            WHERE df_pod=FREIGHT_LETTER_NUMBER
                            AND df_datauftannahme BETWEEN current date-3 AND current date
                            AND df_abstat!='XXX' AND df_empfstat!='XXX'
                            ORDER BY df_datauftannahme DESC";

        cmd.Parameters.Add(new()
        {
            ParameterName = "@FREIGHT_LETTER_NUMBER",
            DbType = System.Data.DbType.String,
            Value = freightLetterNumber
        });

        await using var reader = cmd.ExecuteReader();

        if (reader.HasRows)
        {
            while (reader.Read())
            {
                Console.WriteLine("{0}: {1:C}", reader[0], reader[1]);
            }
        }
        else
        {
            Console.WriteLine("No rows found.");
        }

        reader.Close();

        return (PackageData?) null;
    });

    public Task<ScanData?> GetScanAsync(string freightLetterNumber) => Execute(async connection =>
    {
        var cmd = connection.CreateCommand();

        cmd.CommandText = @"SELECT * FROM DBA.TB_SCAN
                            where df_pod=FREIGHT_LETTER_NUMBER
                            and df_scananlass=30
                            and df_packnr=1
                            and df_abstat='FRA'
                            and df_empfstat='MUC'
                            and df_linnr=53
                            and df_scandat between current date-3 and current date;";

        OdbcParameter param = new()
        {
            ParameterName = "@FREIGHT_LETTER_NUMBER",
            DbType = System.Data.DbType.VarNumeric,
            Value = freightLetterNumber
        };

        cmd.Parameters.Add(param);

        ScanData scanData = null;

        await cmd.ExecuteReaderAsync();

        return scanData;
    });

    public Task AddScanAsync(ScaleDimensionerResult scaleDimensionerResult, PackageData packageData) => Execute(async connection =>
    {
        var cmd = connection.CreateCommand();

        cmd.CommandText = @"INSERT INTO DBA.TB_SCAN
                            (df_abstat, df_empfstat, df_linnr, df_pod, df_packnr, df_scandat, df_scantime,
                            df_scanort, df_scananlass, df_errcode, df_platznr, df_user, df_gewicht, df_kfznr,
                            df_datschicht, df_origdb, df_zieldb, df_zieldb1, df_hub, df_zieldb2, df_timestamp,
                            df_dispoan, df_manuell, df_zieldb_auftraggeber, df_ndl, df_datauftannahme,
                            df_lfdnrauftrag, df_laenge, df_breite, df_hoehe)
                            VALUES
                            (DF_ABSTAT, DF_EMPFSTAT, DF_LINNR, DF_POD, DF_PACKNR, DF_SCANDAT, DF_SCANTIME,
                            DF_SCANORT, DF_SCANANLASS, DF_ERRCODE, DF_PLATZNR, DF_USER, WEIGHT, DF_KFZNR,
                            DF_DATSCHICHT, DF_ORIGDB, DF_ZIELDB, DF_ZIELDB1, DF_HUB, DF_ZIELDB2, DF_TIMESTAMP,
                            DF_DISPOAN, DF_MANUELL, DF_ZIELDB_AUFTRAGGEBER, DF_NDL, DF_DATAUFTANNAHME,
                            DF_LFGNRAUFTRAG, LENGTH, WIDTH, HEIGHT);";

        OdbcParameter weightParam = new()
        {
            ParameterName = "@WEIGHT",
            DbType = System.Data.DbType.VarNumeric,
            Value = scaleDimensionerResult.Weight
        };

        OdbcParameter lengthParam = new()
        {
            ParameterName = "@LENGTH",
            DbType = System.Data.DbType.VarNumeric,
            Value = scaleDimensionerResult.Length
        };

        OdbcParameter widthParam = new()
        {
            ParameterName = "@WIDTH",
            DbType = System.Data.DbType.VarNumeric,
            Value = scaleDimensionerResult.Width
        };

        OdbcParameter heightParam = new()
        {
            ParameterName = "@HEIGTH",
            DbType = System.Data.DbType.VarNumeric,
            Value = scaleDimensionerResult.Height
        };

        cmd.Parameters.Add(weightParam);
        cmd.Parameters.Add(lengthParam);
        cmd.Parameters.Add(widthParam);
        cmd.Parameters.Add(heightParam);

        await cmd.ExecuteNonQueryAsync();
    });

    public Task<int> GetWeightAsync() => Execute(async connection =>
    {
        var cmd = connection.CreateCommand();
        cmd.CommandText = @"SELECT SUM(df_gewicht) AS totalweight
                            FROM DBA.TB_SCAN
                            where df_pod='068007339524'
                            and df_scananlass=30
                            and df_abstat='FRA'
                            and df_empfstat='MUC'
                            and df_linnr=53
                            and df_scandat between current date-3 and current date;";

        await cmd.ExecuteReaderAsync();

        return 0;
    });

    public Task UpdateWeightAsync(int weight, string scanLocation, string date, string orderNumber) => Execute(async connection =>
    {
        var cmd = connection.CreateCommand();

        cmd.CommandText = @"UPDATE DBA.TB_AUFTRAG
                            SET df_real_kg = WEIGHT
                            WHERE df_ndl='SCANLOCATION'
                            and df_datauftannahme='DATE'
                            and df_lfdnrauftrag=ORDERNUMBER;";

        OdbcParameter weightParam = new()
        {
            ParameterName = "@WEIGHT",
            DbType = System.Data.DbType.VarNumeric,
            Value = weight
        };

        OdbcParameter scanLocationParam = new()
        {
            ParameterName = "@SCANLOCATION",
            DbType = System.Data.DbType.String,
            Value = scanLocation
        };

        OdbcParameter dateParam = new()
        {
            ParameterName = "@DATE",
            DbType = System.Data.DbType.String,
            Value = date
        };

        OdbcParameter orderNumberParam = new()
        {
            ParameterName = "@ORDERNUMBER",
            DbType = System.Data.DbType.String,
            Value = orderNumber
        };

        cmd.Parameters.Add(weightParam);
        cmd.Parameters.Add(scanLocationParam);
        cmd.Parameters.Add(dateParam);
        cmd.Parameters.Add(orderNumberParam);

        await cmd.ExecuteNonQueryAsync();
    });

    public Task<bool> DoesPackageExistAsync(string scanLocation, string date, string orderNumber) => Execute(async connection =>
    {
        var cmd = connection.CreateCommand();

        cmd.CommandText = @"SELECT * FROM DBA.TB_AUFTRAGSPACKSTUECK
                            where df_ndl='SCANLOCATION'
                            and df_datauftannahme='DATE'
                            and df_lfdnrauftrag=ORDERNUMBER
                            and df_lfdnrpack = 1;";

        OdbcParameter scanLocationParam = new()
        {
            ParameterName = "@SCANLOCATION",
            DbType = System.Data.DbType.VarNumeric,
            Value = scanLocation
        };

        OdbcParameter dateParam = new()
        {
            ParameterName = "@DATE",
            DbType = System.Data.DbType.VarNumeric,
            Value = date
        };

        OdbcParameter orderNumberParam = new()
        {
            ParameterName = "@ORDERNUMBER",
            DbType = System.Data.DbType.VarNumeric,
            Value = orderNumber
        };

        cmd.Parameters.Add(scanLocationParam);
        cmd.Parameters.Add(dateParam);
        cmd.Parameters.Add(orderNumberParam);

        await using var reader = cmd.ExecuteReader();
        return reader.HasRows;
    });

    public async Task CreatePackageAsync(PackageData data, ScaleDimensionerResult scaleDimensionerResult, float volumeFactor) => Execute(async connection =>
    {
        var cmd = connection.CreateCommand();

        cmd.CommandText = @"INSERT INTO DBA.TB_AUFTRAGSPACKSTUECK (df_ndl, df_datauftannahme,
                            df_lfdnrauftrag, df_lfdnrpack, df_laenge, df_breite, df_hoehe, df_volkg, df_hwb_nr,
                            df_origdb, df_zieldb, df_replikation, df_zieldb1, df_timestamp) VALUES ('TXL', '2019-
                            02-12',551,1,61,51,41, 25.510, '068007339524', current database, 'gomuc', 1, 'gofra',
                            current timestamp);";

        await cmd.ExecuteNonQueryAsync();
    });

    public Task<int> GetTotalWeightAsync(string scanLocation, string date, string orderNumber) => Execute(async connection =>
    {
        var cmd = connection.CreateCommand();

        cmd.CommandText = @"SELECT SUM(df_volkg) AS totalvolumeweight
                            FROM DBA.TB_AUFTRAGSPACKSTUECK
                            where df_ndl='SCANLOCATION'
                            and df_datauftannahme='DATE'
                            and df_lfdnrauftrag=ORDERNUMBER;";


        OdbcParameter scanLocationParam = new()
        {
            ParameterName = "@SCANLOCATION",
            DbType = System.Data.DbType.VarNumeric,
            Value = scanLocation
        };

        OdbcParameter dateParam = new()
        {
            ParameterName = "@DATE",
            DbType = System.Data.DbType.VarNumeric,
            Value = date
        };

        OdbcParameter orderNumberParam = new()
        {
            ParameterName = "@ORDERNUMBER",
            DbType = System.Data.DbType.VarNumeric,
            Value = orderNumber
        };

        cmd.Parameters.Add(scanLocationParam);
        cmd.Parameters.Add(dateParam);
        cmd.Parameters.Add(orderNumberParam);

        await cmd.ExecuteReaderAsync();

        return 0;
    });

    public Task UpdateTotalWeightAsync(int totalWeight, string scanLocation, string date, string orderNumber) => Execute(async connection =>
    {
        var cmd = connection.CreateCommand();

        cmd.CommandText = @"UPDATE DBA.TB_AUFTRAG
                            SET df_volkg = VOLUME
                            WHERE df_ndl='SCANLOCATION'
                            and df_datauftannahme='DATE'
                            and df_lfdnrauftrag=ORDERNUMBER;";

        OdbcParameter totalWeightParam = new()
        {
            ParameterName = "@WEIGHT",
            DbType = System.Data.DbType.VarNumeric,
            Value = totalWeight
        };

        OdbcParameter scanLocationParam = new()
        {
            ParameterName = "@SCANLOCATION",
            DbType = System.Data.DbType.String,
            Value = scanLocation
        };

        OdbcParameter dateParam = new()
        {
            ParameterName = "@DATE",
            DbType = System.Data.DbType.String,
            Value = date
        };

        OdbcParameter orderNumberParam = new()
        {
            ParameterName = "@ORDERNUMBER",
            DbType = System.Data.DbType.String,
            Value = orderNumber
        };

        cmd.Parameters.Add(totalWeightParam);
        cmd.Parameters.Add(scanLocationParam);
        cmd.Parameters.Add(dateParam);
        cmd.Parameters.Add(orderNumberParam);

        await cmd.ExecuteNonQueryAsync();
    });

    public Task UpdateOrderWeightAsync(int weight, string scanLocation, string date, string orderNumber) => Execute(async connection =>
    {
        var cmd = connection.CreateCommand();

        cmd.CommandText = @"UPDATE DBA.TB_AUFTRAG
                            SET df__kg = WEIGHT
                            WHERE df_ndl='SCANLOCATION'
                            and df_datauftannahme='DATE'
                            and df_lfdnrauftrag=ORDERNUMBER;";

        cmd.Parameters.AddRange(new OdbcParameter[]
        {
            new()
            {
                ParameterName = "@WEIGHT",
                DbType = System.Data.DbType.VarNumeric,
                Value = weight
            },
            new()
            {
                ParameterName = "@SCANLOCATION",
                DbType = System.Data.DbType.String,
                Value = scanLocation
            },
            new()
            {
                ParameterName = "@DATE",
                DbType = System.Data.DbType.String,
                Value = date
            },
            new()
            {
                ParameterName = "@ORDERNUMBER",
                DbType = System.Data.DbType.String,
                Value = orderNumber
            }
        });

        await cmd.ExecuteNonQueryAsync();
    });
}