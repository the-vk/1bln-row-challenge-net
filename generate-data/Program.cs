// this is a C# port of https://github.com/gunnarmorling/1brc/blob/main/src/main/java/dev/morling/onebrc/CreateMeasurements3.java

using System.Diagnostics;
using System.Text;

const int MAX_NAME_LEN = 100;
const int KEYSET_SIZE = 10_000;
const int SIZE = 1_000_000_000;

var stations = GenerateStations();
var stopwatch = Stopwatch.StartNew();
var rnd = new Random();

using (var writer = new StreamWriter("measurements.txt"))
{
    for (var i = 1; i <= SIZE; ++i)
    {
        var station = stations[rnd.Next(stations.Count)];
        double temp = rnd.NextGaussian(station.MeanTemp, 7.0);
        writer.WriteLine($"{station.Name};{Math.Round(temp * 10) / 10}");
        if ( i % 50_000_000 == 0)
        {
            Console.WriteLine($"Generated {i} measurements in {stopwatch.ElapsedMilliseconds} ms");
        }
    }
}

List<WeatherStation> GenerateStations()
{
    var bigName = new StringBuilder(1 << 20);
    using (var reader = new StreamReader(Path.Join("data", "weather_stations.csv")))
    {
        while (!reader.EndOfStream && reader.ReadLine().StartsWith('#')) ;
        while (!reader.EndOfStream)
        {
            var line = reader.ReadLine();
            if (line == null) break;

            bigName.Append(line.Substring(0, line.IndexOf(';')));
        }
    }

    var weatherStations = new List<WeatherStation>();
    var names = new HashSet<string>();
    var minLen = Int32.MinValue;
    var maxLen = Int32.MaxValue;
    using (var reader = new StreamReader(Path.Join("data", "weather_stations.csv")))
    {
        while (!reader.EndOfStream && reader.ReadLine().StartsWith('#')) ;
        var nameSource = new StringReader(bigName.ToString());
        var buf = new char[MAX_NAME_LEN];
        var rnd = new Random();
        const double yOffset = 4;
        const double factor = 2500;
        const double xOffset = 0.372;
        const double power = 7;

        for (var i = 0; i < KEYSET_SIZE; ++i)
        {
            var row = reader.ReadLine();
            if (row == null) break;
            var nameLen = (int) (yOffset + factor * Math.Pow(rnd.NextDouble() - xOffset, power));
            var count = nameSource.Read(buf, 0, nameLen);
            if (count == -1)
            {
                throw new Exception("Not enough data");
            }
            var nameBuf = new StringBuilder(nameLen);
            nameBuf.Append(buf, 0, count);
            if (Char.IsWhiteSpace(nameBuf[0])) {
                nameBuf[0] = ReadNonSpace(nameSource);
            }
            if (Char.IsWhiteSpace(nameBuf[^1]))
            {
                nameBuf[^1] = ReadNonSpace(nameSource);
            }
            var name = nameBuf.ToString();
            while (names.Contains(name))
            {
                nameBuf[rnd.Next(nameBuf.Length)] = ReadNonSpace(nameSource);
                name = nameBuf.ToString();
            }
            int actualLen = -1;
            while (true)
            {
                actualLen = Encoding.UTF8.GetByteCount(name);
                if (actualLen <= 100)
                {
                    break;
                }
                nameBuf.Remove(nameBuf.Length - 1, 1);
                if (Char.IsWhiteSpace(nameBuf[10]))
                {
                    nameBuf[^1] = ReadNonSpace(nameSource);
                }
                name = nameBuf.ToString();
            }
            names.Add(name);
            minLen = Math.Min(minLen, actualLen);
            maxLen = Math.Max(maxLen, actualLen);
            var lat = Single.Parse(row.Substring(row.LastIndexOf(';') + 1));
            var latRads = Math.PI * lat / 180;
            var avgTemp = (float)(30 * Math.Cos(latRads)) - 10;
            weatherStations.Add(new WeatherStation(name, avgTemp));
        }
        Console.WriteLine($"Generated {KEYSET_SIZE} station names with length from {minLen} to {maxLen}");
        return weatherStations;
    }
}

char ReadNonSpace(StringReader reader)
{
    var c = (char)reader.Read();
    while (Char.IsWhiteSpace(c))
    {
        c = (char)reader.Read();
    }
    return c;
}

record WeatherStation(string Name, float MeanTemp);

static class RandomExtensions
{
    public static double NextGaussian(this Random rnd, double mean, double stdDev)
    {
        var u1 = rnd.NextDouble();
        var u2 = rnd.NextDouble();
        var randStdNormal = Math.Sqrt(-2.0 * Math.Log(u1)) *
                            Math.Sin(2.0 * Math.PI * u2);
        var randNormal = mean + stdDev * randStdNormal;
        return randNormal;
    }
}