namespace GO.Workerservice;

public class Process 
{
    private readonly DatabaseService _databaseService;
    private readonly Configuration _configuration;

    public Process(Configuration configuration, DatabaseService databaseService) 
    {
        _configuration = configuration;
        _databaseService = databaseService;
    }

    public async Task ProcessPackageAsync(ScaleDimensionerResult scaleDimensionerResult) 
    {
            var freightLetterNumber = scaleDimensionerResult.Barcode;
            var packageData = await _databaseService.GetOrderAsync(freightLetterNumber); // 1
            var scanData = await _databaseService.GetScanAsync(freightLetterNumber); // 2

            var scanLocation = packageData.df_ndl;
            var date = packageData.df_datauftannahme;
            var orderNumber = packageData.df_lfdnrauftrag;

            if (scanData == null)
            {
                await _databaseService.AddScanAsync(scaleDimensionerResult, packageData); // 3

                var realWeight = await _databaseService.GetWeightAsync(); // 4

                if (realWeight == null) return;

                await _databaseService.UpdateWeightAsync((int) realWeight, scanLocation, date, orderNumber); // 5

                var packageExists = await _databaseService.DoesPackageExistAsync(scanLocation, date, orderNumber); // 6

                if (packageExists == null) return;

                float volumeFactor;

                if (packageData.df_ndl == _configuration.ScanLocation // 7
                    && _configuration.ExceptionList!.ContainsKey(packageData.df_kundennr)) 
                {
                    volumeFactor = _configuration.ExceptionList[packageData.df_kundennr];
                } else {
                    volumeFactor = _configuration.DefaultVolumeFactor;
                }

                await _databaseService.CreatePackageAsync(packageData, scaleDimensionerResult, volumeFactor); // 8

                var volumeWeight = await _databaseService.GetTotalWeightAsync(packageData.df_ndl,   // 9
                                                                                  packageData.df_datauftannahme, 
                                                                                  packageData.df_lfdnrauftrag);

                if (volumeWeight == null) return;

                await _databaseService.UpdateTotalWeightAsync((int) volumeWeight, scanLocation, date, orderNumber);

                var weight = (int) realWeight;

                if (packageData.df_ndl == _configuration.ScanLocation && volumeWeight > realWeight) {
                    weight = (int) volumeWeight;
                } 

                await _databaseService.UpdateOrderWeightAsync((int) realWeight, scanLocation, date, orderNumber);
            }
    }
}