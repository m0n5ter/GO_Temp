using System.Globalization;

namespace GO.Workerservice;

public enum Shape 
{
    UnknownShape = 0x00,
    Cylinder = 0x01,
    Cubic = 0x04,
    Tyre = 0x07
}

public enum ScaleState 
{
    NotOk = -1,
    Ok = 0,
    Unstable = 1,
    MultipleItems = 3,
    Underloaded = 4,
    Overloaded = 5,
}

public enum DimensionerState 
{
    LFT = 0,
    NoLFT = 1
}

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
    
    public int Length { get; }
    
    public int Width { get; }
    
    public int Height { get; }

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

    public double Volume { get; }
    
    private static decimal TryParseDecimal(string s, string name) => decimal.TryParse(s, CultureInfo.InvariantCulture, out var d) ? d : throw new Exception($"Failed to parse {name}: {s}");

    private static int TryParseInt(string s, string name) => int.TryParse(s, CultureInfo.InvariantCulture, out var i) ? i : throw new Exception($"Failed to parse {name}: {s}");

    public ScaleDimensionerResult(string data)
    {
        if (data.Length != 92) throw new Exception($"Unexpected message length: {data.Length}");

        if (data[0] != 'W') throw new Exception("Weight flag missing");
        if (data[7] != 'V') throw new Exception("Volume flag missing");
        if (data[20] != 'B') throw new Exception("Barcode flag missing");

        Weight = TryParseDecimal(data[1..6], nameof(Weight));
        
        Length = TryParseInt(data[8..12], nameof(Length));
        Width = TryParseInt(data[12..16], nameof(Width));
        Height = TryParseInt(data[16..20], nameof(Height));
        
        Volume = Width * Height * Length;

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