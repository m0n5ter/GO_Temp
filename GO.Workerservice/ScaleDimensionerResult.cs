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
    public double Weight { get; }
    
    public double Length { get; }
    
    public double Width { get; }
    
    public double Height { get; }

    public string Barcode { get; }

    public string ToStation { get; set; }

    public string FromStation { get; }

    public string LineNumber { get; }

    public ScaleState ScaleState { get; }

    public DimensionerState DimensionerState { get; }

    public Shape Shape { get; }

    public bool ScaleLFT { get; }

    public bool DimensionerLFT { get; }

    public double Volume { get; }
    
    private static double TryParseDouble(string s, string name) => double.TryParse(s, CultureInfo.InvariantCulture, out var d) ? d : throw new Exception($"Failed to parse {name}: {s}");

    private static int TryParseInt(string s, string name) => int.TryParse(s, CultureInfo.InvariantCulture, out var ds) ? ds : throw new Exception($"Failed to parse {name}: {s}");

    public ScaleDimensionerResult(string data)
    {
        if (data.Length != 92) throw new Exception($"Unexpected message length: {data.Length}");

        if (data[0] != 'W') throw new Exception("Weight flag missing");
        if (data[7] != 'V') throw new Exception("Volume flag missing");
        if (data[20] != 'B') throw new Exception("Barcode flag missing");

        Weight = TryParseDouble(data[1..6], nameof(Weight));
        Length = TryParseDouble(data[8..11], nameof(Length));
        Width = TryParseDouble(data[12..15], nameof(Width));
        Height = TryParseDouble(data[16..19], nameof(Height));
        Volume = Width * Height * Length;
        Barcode = data[41..80].TrimStart('#', ' ');
        FromStation = Barcode[..3];
        ToStation = Barcode[3..6];
        LineNumber = Barcode[7..9];

        // TODO: The values below are probably wrong, need to test

        DimensionerState = (DimensionerState) TryParseInt(data[81..85], nameof(DimensionerState));
        ScaleState = (ScaleState) char.GetNumericValue(data[85]) - '0';
        Shape = (Shape) TryParseInt(data[86..87], nameof(Shape));
        ScaleLFT = data[88] == '1';
        DimensionerLFT = data[89] == '1';
    }
}