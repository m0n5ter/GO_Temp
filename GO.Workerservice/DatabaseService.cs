using System.Globalization;

namespace GO.Workerservice;

using Microsoft.Extensions.Logging;
using System;
using System.Data.Odbc;

public class DatabaseService
{
    private const int DAYS_TO_CONSIDER = 100;

    private readonly Configuration _configuration;
    private readonly ILogger<DatabaseService> _logger;
    private OdbcConnection? _connection;
    private OdbcTransaction? _transaction;

    public DatabaseService(Configuration configuration, ILogger<DatabaseService> logger)
    {
        _configuration = configuration;
        _logger = logger;
    }

    public async Task Begin()
    {
        _connection = new OdbcConnection {ConnectionString = _configuration.DatabaseConfiguration.ConnectionString};
        await _connection.OpenAsync();
        _transaction = (OdbcTransaction?) await _connection.BeginTransactionAsync();
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

    private OdbcCommand BuildCommand(string sql, params OdbcParameter[] parameters)
    {
        if (_transaction == null) throw new InvalidOperationException("Call Begin() first");

        var command = new OdbcCommand(sql, _connection, _transaction);
        command.Parameters.AddRange(parameters);
        return command;
    }

    public async Task<OrderData?> GetOrderAsync(ScaleDimensionerResult scan)
    {
        var sql = @$"
SELECT FIRST 
  *,
  (SELECT IF DF_KZ_GO>0 then 'go'+lower(DF_HSTAT) ELSE null ENDIF FROM DBA.TB_STATIONEN WHERE DF_STAT = DF_ABSTAT) zieldb1,
  (SELECT IF DF_KZ_GO>0 then 'go'+lower(DF_HSTAT) ELSE null ENDIF FROM DBA.TB_STATIONEN WHERE DF_STAT=DF_EMPFSTAT) zieldb
FROM DBA.TB_AUFTRAG
WHERE DF_POD='{scan.OrderNumber}'
  AND DF_DATAUFTANNAHME BETWEEN current date-{DAYS_TO_CONSIDER} AND current date
  AND DF_ABSTAT!='XXX' AND DF_EMPFSTAT!='XXX'
ORDER BY DF_DATAUFTANNAHME DESC";

        await using var reader = await BuildCommand(sql).ExecuteReaderAsync();

        if (!reader.HasRows) return null;
        
        await reader.ReadAsync();

        return new OrderData(reader);
    }

    public async Task<bool> ScanExistsAsync(ScaleDimensionerResult scan)
    {
        var sql = @$"
SELECT 
  FIRST DF_POD 
FROM DBA.TB_SCAN
WHERE 
  DF_SCANANLASS=30 AND 
  DF_SCANDAT between current date-{DAYS_TO_CONSIDER} AND current date AND
  DF_POD='{scan.OrderNumber}' AND 
  DF_PACKNR='{scan.PackageNumber}' AND 
  DF_ABSTAT='{scan.FromStation}' AND
  DF_EMPFSTAT='{scan.ToStation}' AND
  DF_LINNR='{scan.LineNumber}'";

        await using var reader = await BuildCommand(sql).ExecuteReaderAsync();
        return (await BuildCommand(sql).ExecuteReaderAsync()).HasRows;
    }

    public async Task AddScanAsync(OrderData order, ScaleDimensionerResult scan)
    {
        var sql = @$"
INSERT INTO DBA.TB_SCAN
  (DF_ABSTAT, DF_EMPFSTAT, DF_LINNR, DF_POD, DF_PACKNR, DF_SCANDAT, DF_SCANTIME,
  DF_SCANORT, DF_SCANANLASS, DF_ERRCODE, DF_PLATZNR, DF_USER, DF_GEWICHT, DF_KFZNR,
  DF_DATSCHICHT, DF_ORIGDB, DF_ZIELDB, DF_ZIELDB1, DF_HUB, DF_ZIELDB2, DF_TIMESTAMP,
  DF_DISPOAN, DF_MANUELL, DF_ZIELDB_AUFTRAGGEBER, DF_NDL, DF_DATAUFTANNAHME,
  DF_LFDNRAUFTRAG, DF_LAENGE, DF_BREITE, DF_HOEHE)
VALUES
  ('{scan.FromStation}', '{scan.ToStation}', '{scan.LineNumber}', '{scan.OrderNumber}', {scan.PackageNumber}, current date, current time, 
  '{_configuration.ScanLocation}', 30, null, 0, '{_configuration.ScanLocation}', {scan.Weight.ToString(CultureInfo.InvariantCulture)}, null,
  current date, current database, '{order.zieldb}', '{order.zieldb1}', '{order.DF_HUB}', null, current timestamp,
  0, 'N', null, '{order.DF_NDL}', current date,
  {order.DF_LFDNRAUFTRAG}, {scan.Length}, {scan.Width}, {scan.Height})";

        await BuildCommand(sql).ExecuteNonQueryAsync();
    }

    public async Task<decimal> GetTotalWeightAsync(ScaleDimensionerResult scan)
    {
        var sql = $@"
SELECT 
  SUM(DF_GEWICHT)
FROM DBA.TB_SCAN
WHERE 
  DF_SCANANLASS=30 AND
  DF_SCANDAT between current date-{DAYS_TO_CONSIDER} AND current date AND
  DF_POD='{scan.OrderNumber}' AND
  DF_ABSTAT='{scan.FromStation}' AND
  DF_EMPFSTAT='{scan.ToStation}' AND
  DF_LINNR='{scan.LineNumber}'";

        return await BuildCommand(sql).ExecuteScalarAsync() is decimal d ? d : 0;
    }

    public async Task SetTotalWeightAsync(OrderData order, decimal totalWeight)
    {
        var sql = @$"
UPDATE 
  DBA.TB_AUFTRAG 
SET 
  df_real_kg={totalWeight.ToString(CultureInfo.InvariantCulture)} 
WHERE 
  DF_NDL='{order.DF_NDL}' AND
  DF_DATAUFTANNAHME='{order.DF_DATAUFTANNAHME:yyyy-MM-dd}' AND
  df_lfdnrauftrag={order.DF_LFDNRAUFTRAG}";

        await BuildCommand(sql).ExecuteNonQueryAsync();
    }

    //public Task<bool> DoesPackageExistAsync(string scanLocation, string date, string orderNumber) => Execute(async connection =>
    //{
    //    var cmd = connection.CreateCommand();

    //    cmd.CommandText = @"SELECT * FROM DBA.TB_AUFTRAGSPACKSTUECK
    //                        where df_ndl='SCANLOCATION'
    //                        and df_datauftannahme='DATE'
    //                        and df_lfdnrauftrag=ORDERNUMBER
    //                        and df_lfdnrpack = 1;";

    //    OdbcParameter scanLocationParam = new()
    //    {
    //        ParameterName = "@SCANLOCATION",
    //        DbType = System.Data.DbType.VarNumeric,
    //        Value = scanLocation
    //    };

    //    OdbcParameter dateParam = new()
    //    {
    //        ParameterName = "@DATE",
    //        DbType = System.Data.DbType.VarNumeric,
    //        Value = date
    //    };

    //    OdbcParameter orderNumberParam = new()
    //    {
    //        ParameterName = "@ORDERNUMBER",
    //        DbType = System.Data.DbType.VarNumeric,
    //        Value = orderNumber
    //    };

    //    cmd.Parameters.Add(scanLocationParam);
    //    cmd.Parameters.Add(dateParam);
    //    cmd.Parameters.Add(orderNumberParam);

    //    await using var reader = cmd.ExecuteReader();
    //    return reader.HasRows;
    //});

    //public Task CreatePackageAsync(PackageData data, ScaleDimensionerResult scaleDimensionerResult, float volumeFactor) => Execute(async connection =>
    //{
    //    var cmd = connection.CreateCommand();

    //    cmd.CommandText = @"INSERT INTO DBA.TB_AUFTRAGSPACKSTUECK (df_ndl, df_datauftannahme,
    //                        df_lfdnrauftrag, df_lfdnrpack, df_laenge, df_breite, df_hoehe, df_volkg, df_hwb_nr,
    //                        df_origdb, df_zieldb, df_replikation, df_zieldb1, df_timestamp) VALUES ('TXL', '2019-
    //                        02-12',551,1,61,51,41, 25.510, '068007339524', current database, 'gomuc', 1, 'gofra',
    //                        current timestamp);";

    //    await cmd.ExecuteNonQueryAsync();
    //});

    //public Task<int> GetTotalWeightAsync(string scanLocation, string date, string orderNumber) => Execute(async connection =>
    //{
    //    var cmd = connection.CreateCommand();

    //    cmd.CommandText = @"SELECT SUM(df_volkg) AS totalvolumeweight
    //                        FROM DBA.TB_AUFTRAGSPACKSTUECK
    //                        where df_ndl='SCANLOCATION'
    //                        and df_datauftannahme='DATE'
    //                        and df_lfdnrauftrag=ORDERNUMBER;";


    //    OdbcParameter scanLocationParam = new()
    //    {
    //        ParameterName = "@SCANLOCATION",
    //        DbType = System.Data.DbType.VarNumeric,
    //        Value = scanLocation
    //    };

    //    OdbcParameter dateParam = new()
    //    {
    //        ParameterName = "@DATE",
    //        DbType = System.Data.DbType.VarNumeric,
    //        Value = date
    //    };

    //    OdbcParameter orderNumberParam = new()
    //    {
    //        ParameterName = "@ORDERNUMBER",
    //        DbType = System.Data.DbType.VarNumeric,
    //        Value = orderNumber
    //    };

    //    cmd.Parameters.Add(scanLocationParam);
    //    cmd.Parameters.Add(dateParam);
    //    cmd.Parameters.Add(orderNumberParam);

    //    await cmd.ExecuteReaderAsync();

    //    return 0;
    //});

    //public Task UpdateTotalWeightAsync(int totalWeight, string scanLocation, string date, string orderNumber) => Execute(async connection =>
    //{
    //    var cmd = connection.CreateCommand();

    //    cmd.CommandText = @"UPDATE DBA.TB_AUFTRAG
    //                        SET df_volkg = VOLUME
    //                        WHERE df_ndl='SCANLOCATION'
    //                        and df_datauftannahme='DATE'
    //                        and df_lfdnrauftrag=ORDERNUMBER;";

    //    OdbcParameter totalWeightParam = new()
    //    {
    //        ParameterName = "@WEIGHT",
    //        DbType = System.Data.DbType.VarNumeric,
    //        Value = totalWeight
    //    };

    //    OdbcParameter scanLocationParam = new()
    //    {
    //        ParameterName = "@SCANLOCATION",
    //        DbType = System.Data.DbType.String,
    //        Value = scanLocation
    //    };

    //    OdbcParameter dateParam = new()
    //    {
    //        ParameterName = "@DATE",
    //        DbType = System.Data.DbType.String,
    //        Value = date
    //    };

    //    OdbcParameter orderNumberParam = new()
    //    {
    //        ParameterName = "@ORDERNUMBER",
    //        DbType = System.Data.DbType.String,
    //        Value = orderNumber
    //    };

    //    cmd.Parameters.Add(totalWeightParam);
    //    cmd.Parameters.Add(scanLocationParam);
    //    cmd.Parameters.Add(dateParam);
    //    cmd.Parameters.Add(orderNumberParam);

    //    await cmd.ExecuteNonQueryAsync();
    //});

    //public Task UpdateOrderWeightAsync(int weight, string scanLocation, string date, string orderNumber) => Execute(async connection =>
    //{
    //    var cmd = connection.CreateCommand();

    //    cmd.CommandText = @"UPDATE DBA.TB_AUFTRAG
    //                        SET df__kg = WEIGHT
    //                        WHERE df_ndl='SCANLOCATION'
    //                        and df_datauftannahme='DATE'
    //                        and df_lfdnrauftrag=ORDERNUMBER;";

    //    cmd.Parameters.AddRange(new OdbcParameter[]
    //    {
    //        new()
    //        {
    //            ParameterName = "@WEIGHT",
    //            DbType = System.Data.DbType.VarNumeric,
    //            Value = weight
    //        },
    //        new()
    //        {
    //            ParameterName = "@SCANLOCATION",
    //            DbType = System.Data.DbType.String,
    //            Value = scanLocation
    //        },
    //        new()
    //        {
    //            ParameterName = "@DATE",
    //            DbType = System.Data.DbType.String,
    //            Value = date
    //        },
    //        new()
    //        {
    //            ParameterName = "@ORDERNUMBER",
    //            DbType = System.Data.DbType.String,
    //            Value = orderNumber
    //        }
    //    });

    //    await cmd.ExecuteNonQueryAsync();
    //});
}