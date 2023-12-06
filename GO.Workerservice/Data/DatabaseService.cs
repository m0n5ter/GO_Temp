using System.Data.Odbc;
using System.Globalization;
using GO.Workerservice.Config;
using GO.Workerservice.Model;

namespace GO.Workerservice.Data;

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

    public async Task UpdateScanAsync(ScaleDimensionerResult scan)
    {
        var sql = @$"
UPDATE DBA.TB_SCAN SET
  DF_GEWICHT={scan.Weight.ToString(CultureInfo.InvariantCulture)},
  DF_LAENGE={scan.Length},
  DF_BREITE={scan.Width},
  DF_HOEHE={scan.Height},
  DF_TIMESTAMP=current timestamp
WHERE 
  DF_SCANANLASS=30 AND 
  DF_SCANDAT between current date-{DAYS_TO_CONSIDER} AND current date AND
  DF_POD='{scan.OrderNumber}' AND 
  DF_PACKNR='{scan.PackageNumber}' AND 
  DF_ABSTAT='{scan.FromStation}' AND
  DF_EMPFSTAT='{scan.ToStation}' AND
  DF_LINNR='{scan.LineNumber}'";

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

    public async Task SetOrderWeightsAsync(OrderData order, decimal realWeight, decimal volumeWeight, decimal chargeableWeight)
    {
        var sql = @$"
UPDATE DBA.TB_AUFTRAG SET 
  DF_REAL_KG={realWeight.ToString(CultureInfo.InvariantCulture)},
  DF_VOLKG={volumeWeight.ToString(CultureInfo.InvariantCulture)},
  DF_KG={chargeableWeight.ToString(CultureInfo.InvariantCulture)}
WHERE 
  DF_NDL='{order.DF_NDL}' AND
  DF_DATAUFTANNAHME='{order.DF_DATAUFTANNAHME:yyyy-MM-dd}' AND
  DF_LFDNRAUFTRAG={order.DF_LFDNRAUFTRAG}";

        await BuildCommand(sql).ExecuteNonQueryAsync();
    }

    public async Task<bool> PackageExistsAsync(OrderData order, ScaleDimensionerResult scan)
    {
        var sql = $@"
SELECT * 
FROM DBA.TB_AUFTRAGSPACKSTUECK 
WHERE 
  DF_NDL='{order.DF_NDL}' AND
  DF_DATAUFTANNAHME='{order.DF_DATAUFTANNAHME:yyyy-MM-dd}' AND
  DF_LFDNRAUFTRAG={order.DF_LFDNRAUFTRAG} AND
  DF_LFDNRPACK={scan.PackageNumber}";

        return (await BuildCommand(sql).ExecuteReaderAsync()).HasRows;
    }
    
    public async Task AddPackageAsync(OrderData order, ScaleDimensionerResult scan, decimal volumeFactor)
    {
        var sql = $@"
INSERT INTO DBA.TB_AUFTRAGSPACKSTUECK 
  (DF_NDL, DF_DATAUFTANNAHME, DF_LFDNRAUFTRAG, DF_LFDNRPACK,  DF_LAENGE, DF_BREITE, DF_HOEHE, DF_VOLKG, 
  DF_HWB_NR, DF_ORIGDB, DF_ZIELDB, DF_REPLIKATION, DF_ZIELDB1, DF_TIMESTAMP) 
VALUES 
  ('{order.DF_NDL}', '{order.DF_DATAUFTANNAHME:yyyy-MM-dd}', {order.DF_LFDNRAUFTRAG}, {scan.PackageNumber}, {scan.Length}, {scan.Width}, {scan.Height}, {(scan.Volume / volumeFactor).ToString(CultureInfo.InvariantCulture)}, 
  '{order.DF_POD}', current database, '{order.zieldb}', 1, '{order.zieldb1}', current timestamp)";

        await BuildCommand(sql).ExecuteNonQueryAsync();
    }

    public async Task UpdatePackageAsync(OrderData order, ScaleDimensionerResult scan, decimal volumeFactor)
    {
        var sql = $@"
UPDATE DBA.TB_AUFTRAGSPACKSTUECK SET
  DF_VOLKG={(scan.Volume / volumeFactor).ToString(CultureInfo.InvariantCulture)},
  DF_LAENGE={scan.Length},
  DF_BREITE={scan.Width},
  DF_HOEHE={scan.Height},
  DF_TIMESTAMP=current timestamp
WHERE 
  DF_NDL='{order.DF_NDL}' AND
  DF_DATAUFTANNAHME='{order.DF_DATAUFTANNAHME:yyyy-MM-dd}' AND
  DF_LFDNRAUFTRAG={order.DF_LFDNRAUFTRAG} AND
  DF_LFDNRPACK={scan.PackageNumber}";

        await BuildCommand(sql).ExecuteNonQueryAsync();
    }

    public async Task<decimal> GetTotalVolumeWeightAsync(OrderData order)
    {
        var sql = $@"
SELECT 
  SUM(DF_VOLKG)
FROM DBA.TB_AUFTRAGSPACKSTUECK 
WHERE
  DF_NDL='{order.DF_NDL}' AND
  DF_DATAUFTANNAHME='{order.DF_DATAUFTANNAHME:yyyy-MM-dd}' AND
  DF_LFDNRAUFTRAG={order.DF_LFDNRAUFTRAG}";

        return await BuildCommand(sql).ExecuteScalarAsync() is decimal d ? d : 0;
    }
}