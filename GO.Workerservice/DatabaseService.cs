namespace GO.Workerservice;

using Microsoft.Extensions.Logging;
using System.Data.Common;
using System.Data.Odbc;

public class DatabaseService
{
    private const int DAYS_TO_CONSIDER = 100;

    private readonly DatabaseConfiguration _databaseConfiguration;
    private readonly ILogger<DatabaseService> _logger;
    private OdbcConnection? _connection;
    private DbTransaction? _transaction;

    public DatabaseService(Configuration configuration, ILogger<DatabaseService> logger)
    {
        _databaseConfiguration = configuration.DatabaseConfiguration;
        _logger = logger;
    }

    public async Task Begin()
    {
        _connection = new OdbcConnection {ConnectionString = _databaseConfiguration.ConnectionString};
        await _connection.OpenAsync();
        _transaction = await _connection.BeginTransactionAsync();
    }

    private async Task End(bool commit)
    {
        if (_transaction != null)
        {
            if (commit)
                await _transaction.CommitAsync();
            else
                await _transaction.RollbackAsync();

            _transaction = null;
        }

        if (_connection != null)
        {
            await _connection.CloseAsync();
            _connection = null;
        }
    }

    public Task Commit() => End(true);

    public Task Rollback() => End(false);

    private async Task<T> Execute<T>(Func<OdbcConnection, Task<T>> task)
    {
        if (_connection == null) throw new InvalidOperationException("Call Begin() first");
           return await task(_connection);
    }

    private async Task Execute(Func<OdbcConnection, Task> action) => await Execute(new Func<OdbcConnection, Task<object?>>(async c =>
    {
        await action(c);
        return null;
    }));

    public Task<OrderData?> GetOrderAsync(ScaleDimensionerResult scan) => Execute(async connection =>
    {
        var cmd = new OdbcCommand(@$"
SELECT FIRST 
  *,
  (SELECT IF df_kz_go>0 then 'go'+lower(df_hstat) ELSE null ENDIF FROM dba.tb_stationen WHERE df_stat = df_abstat) zieldb1,
  (SELECT IF df_kz_go>0 then 'go'+lower(df_hstat) ELSE null ENDIF FROM dba.tb_stationen WHERE df_stat=df_empfstat) zieldb,
  current database origdb
FROM DBA.TB_AUFTRAG
WHERE df_pod=?
  AND df_datauftannahme BETWEEN current date-{DAYS_TO_CONSIDER} AND current date
  AND df_abstat!='XXX' AND df_empfstat!='XXX'
ORDER BY df_datauftannahme DESC", connection)
        {
            Parameters =
            {
                new("ORDER_NUMBER", OdbcType.Text) {Value = scan.OrderNumber}
            }
        };

        await using var reader = await cmd.ExecuteReaderAsync();

        if (!reader.HasRows) return null;
        
        await reader.ReadAsync();

        return new OrderData(reader);
    });

    public Task<ScanData2?> GetScanAsync(ScaleDimensionerResult scan) => Execute(async connection =>
    {
        var cmd = new OdbcCommand(@$"
SELECT * FROM DBA.TB_SCAN
WHERE 
  df_scananlass=30 AND 
  df_scandat between current date-{DAYS_TO_CONSIDER} AND current date AND
  df_pod=? AND 
  df_packnr=? AND 
  df_abstat=? AND
  df_empfstat=? AND
  df_linnr=?
  ", connection)
        {
            Parameters =
            {
                new("@ORDER_NUMBER", OdbcType.Text) {Value = scan.OrderNumber},
                new("@PACKAGE_NUMBER", OdbcType.Int) {Value = scan.PackageNumber},
                new("@FROM_STATION", OdbcType.Text) {Value = scan.FromStation},
                new("@TO_STATION", OdbcType.Text) {Value = scan.ToStation},
                new("@LINE_NUMBER", OdbcType.Text) {Value = scan.LineNumber}
            }
        };

        await using var reader = await cmd.ExecuteReaderAsync();

        if (!reader.HasRows) return null;

        await reader.ReadAsync();

        return new ScanData2(reader);
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

    public Task CreatePackageAsync(PackageData data, ScaleDimensionerResult scaleDimensionerResult, float volumeFactor) => Execute(async connection =>
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