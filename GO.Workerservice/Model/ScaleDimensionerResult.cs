using System.Globalization;

namespace GO.Workerservice.Model;

/*
    Byte        Name
    1           Gewichtsflag
    2 - 7       Gewicht
    8           Volumenflag
    9 - 20      Volumen
    21          Bar code flag
    42 - 81     Barcode
    82 - 85     Statuscode Dimensionierer
    86          Status Waage
    87 - 88     Known shape
    89          Waage LFT
    90          Volumen LFT
    91          CR
*/

public sealed class ScaleDimensionerResult
{
    public decimal Weight { get; }
    
    public int LengthCm { get; }
    
    public int WidthCm { get; }
    
    public int HeightCm { get; }

    public string Barcode { get; }

    public string OrderNumber { get; }

    public int PackageNumber { get; }

    public string ToStation { get; set; }

    public string FromStation { get; }

    public string LineNumber { get; }

    public ScaleState ScaleState { get; }

    public DimensionerState DimensionerState { get; }

    public Shape Shape { get; }

    public bool ScaleLFT { get; }

    public bool DimensionerLFT { get; }

    public decimal VolumeM3 { get; }
    
    private static decimal TryParseDecimal(string s, string name) => decimal.TryParse(s, CultureInfo.InvariantCulture, out var d) ? d : throw new Exception($"Failed to parse {name}: {s}");

    private static int TryParseInt(string s, string name) => int.TryParse(s, CultureInfo.InvariantCulture, out var i) ? i : throw new Exception($"Failed to parse {name}: {s}");

    public override string ToString() => $"{FromStation}==>{ToStation}({LineNumber})|{OrderNumber}:{PackageNumber}|{WidthCm}x{LengthCm}x{HeightCm}cm|{VolumeM3}m3|{Weight}kg";

    public ScaleDimensionerResult(string data)
    {
        if (data.Length != 91) throw new Exception($"Unexpected message length: {data.Length}");

        if (data[0] != 'W') throw new Exception("Weight flag missing");
        if (data[7] != 'V') throw new Exception("Volume flag missing");
        if (data[20] != 'B') throw new Exception("Barcode flag missing");

        Weight = TryParseDecimal(data[1..7], nameof(Weight));

        LengthCm = (int)Math.Round(TryParseInt(data[8..12], nameof(LengthCm)) / 10d);
        WidthCm = (int)Math.Round(TryParseInt(data[12..16], nameof(WidthCm)) / 10d);
        HeightCm = (int)Math.Round(TryParseInt(data[16..20], nameof(HeightCm)) / 10d);
        
        VolumeM3 = WidthCm * HeightCm * LengthCm / 1000000m;

        Barcode = data[41..81].TrimStart('#', ' ');
        if (Barcode.Length != 22) throw new Exception($"Unexpected barcode length: {Barcode.Length}");

        FromStation = Barcode[..3];
        ToStation = Barcode[3..6];
        LineNumber = Barcode[6..8];
        OrderNumber = Barcode[8..20].TrimStart('0');
        PackageNumber = int.TryParse(Barcode[20..22], out var packageNumber) ? packageNumber : throw new Exception($"Unexpected package number: {Barcode[20..22]}");

        // TODO: The values below are probably wrong, need to test

        DimensionerState = (DimensionerState) TryParseInt(data[81..85], nameof(DimensionerState));
        ScaleState = (ScaleState) char.GetNumericValue(data[85]) - '0';
        Shape = (Shape) TryParseInt(data[86..87], nameof(Shape));
        ScaleLFT = data[88] == '1';
        DimensionerLFT = data[89] == '1';
    }
}