using System.Diagnostics;
using System.IO.MemoryMappedFiles;

namespace mmap_fil
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            var cpus = Environment.ProcessorCount;

            const string path = "measurements.txt";
            var fileInfo = new FileInfo(path);
            var fileSize = fileInfo.Length;
            var segmentSize = fileSize / cpus;
            var mmapFile = MemoryMappedFile.CreateFromFile(path, FileMode.Open);

            var threads = new Thread[cpus];
            var intermediate = new Dictionary<string, WeatherStation>[cpus];
            for (var i = 0; i < cpus; i++)
            {
                threads[i] = new Thread(ProcessSegment);
                threads[i].IsBackground = true;
                threads[i].Name = $"worker #{i}";
                var view = mmapFile.CreateViewStream();
                threads[i].Start(Tuple.Create<int, Stream, long, long, Dictionary<string, WeatherStation>[]>(i, view, segmentSize * i, segmentSize * (i + 1), intermediate));
            }
            foreach (var thread in threads)
            {
                thread.Join();
            }
            Console.WriteLine($"[{stopwatch.ElapsedMilliseconds}ms] All threads finished. Merging results...");
            for (var i = 1; i < cpus; ++i)
            {
                foreach (var (key, value) in intermediate[i])
                {
                    if (!intermediate[0].ContainsKey(key))
                    {
                        intermediate[0][key] = value;
                    }
                    else
                    {
                        var ws = intermediate[0][key];
                        ws.MinTemp = Math.Min(ws.MinTemp, value.MinTemp);
                        ws.MaxTemp = Math.Max(ws.MaxTemp, value.MaxTemp);
                        ws.MeanTemp = (ws.MeanTemp * ws.Count + value.MeanTemp * value.Count) / (ws.Count + value.Count);
                        ws.Count += value.Count;
                    }
                }
            }
            Console.WriteLine($"[{stopwatch.ElapsedMilliseconds}ms] Writing results...");
            var stations = intermediate[0].Keys.Order().ToList();
            using (var streamWriter = new StreamWriter("result.txt", false))
            {
                streamWriter.Write("{");
                for (var i = 0; i < stations.Count; ++i)
                {
                    var measure = intermediate[0][stations[i]];
                    streamWriter.Write($"{stations[i]}={measure.MinTemp.ToString("F2")}/{measure.MeanTemp.ToString("F2")}/{measure.MaxTemp.ToString("F2")}");
                    if (i < stations.Count - 1)
                    {
                        streamWriter.Write(", ");
                    }
                }
            }
            Console.WriteLine($"[{stopwatch.ElapsedMilliseconds}ms] Done. Processed {intermediate[0].Values.Select(x => x.Count).Aggregate((l, r) => l + r)} rows.");
        }

        static void ProcessSegment(object? arg)
        {
            if (null == arg)
            {
                throw new ArgumentNullException(nameof(arg));
            }
            var stopwatch = Stopwatch.StartNew();
            var (index, stream, start, end, intermediate) = (Tuple<int, Stream, long, long, Dictionary<string, WeatherStation>[]>)arg;
            intermediate[index] = new Dictionary<string, WeatherStation>();
            var store = intermediate[index];
            stream.Seek(start, SeekOrigin.Begin);
            var reader = new StreamReader(stream);
            if (start > 0)
            {
                reader.ReadLine();
            }
            long count = 0;
            while (stream.Position < end)
            {
                var line = reader.ReadLine();
                if (line == null) break;
                var fields = line.Split(';');
                var station = fields[0];
                var temperature = Double.Parse(fields[1]);                
                
                if (!store.ContainsKey(station))
                {
                    store[station] = new WeatherStation
                    {
                        Count = 1,
                        MinTemp = temperature,
                        MeanTemp = temperature,
                        MaxTemp = temperature
                    };
                }
                else
                {
                    var ws = store[station];
                    ws.MinTemp = Math.Min(ws.MinTemp, temperature);
                    ws.MaxTemp = Math.Max(ws.MaxTemp, temperature);
                    ws.MeanTemp = (ws.MeanTemp * ws.Count + temperature) / (ws.Count + 1);
                    ws.Count++;
                }
                ++count;
                if (count % 1_000_000 == 0)
                {
                    Console.WriteLine($"[{stopwatch.ElapsedMilliseconds}ms] worker #{index} processed {count} lines at {((double)count / stopwatch.ElapsedMilliseconds) * 1000.0}l/s");
                }
            }
        }
    }

    class WeatherStation
    {
        public long Count { get; set; }
        public double MinTemp { get; set; }
        public double MeanTemp { get; set; }
        public double MaxTemp { get; set; }
    }
}
