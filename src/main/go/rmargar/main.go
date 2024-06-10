package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
)

const (
	separator          = ";"
	defaultChunkSizeMB = 128
	mb                 = 1024 * 1024
)

type measurement struct {
	min, max, mean, sum float64
	count               int32
}

func parseArgs() (string, bool) {
	filename := flag.String("filename", "", "File to process")
	profile := flag.Bool("profile", true, "profile")
	flag.Parse()
	return *filename, *profile
}

func main() {
	filename, profile := parseArgs()

	var numProcessors int
	var parseChunkSize int

	if profile {
		f, _ := os.Create("1br.prof")
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	log.Println("Processing file: ", filename)
	file, err := os.Open(filename)
	parseChunkSize = defaultChunkSizeMB * mb

	if err != nil {
		fmt.Println("Error opening file: ", err)
		os.Exit(1)
	}

	// Add the number of threads to the waiting group
	numProcessors = runtime.NumCPU()

	//define results channel

	// close the file when the function ends
	defer func() {
		err = file.Close()
		if err != nil {
			fmt.Println("Error closing file: ", err)
			os.Exit(1)
		}
	}()

	printResults(process(file, numProcessors, parseChunkSize))
}

func process(file *os.File, numWorkers int, chunkSize int) map[string]*measurement {
	var wg sync.WaitGroup

	info, err := file.Stat()
	if err != nil {
		panic("Error while getting file stat")
	}
	// buffered to not block on merging - size == numWorkers
	chunkOffsetCh := make(chan int64, numWorkers)
	chunkMeasurementsCh := make(chan map[string]*measurement, numWorkers)

	go func() {
		i := 0
		for i < int(info.Size()) {
			chunkOffsetCh <- int64(i)
			i += chunkSize
		}
		close(chunkOffsetCh)
	}()

	for i := 0; i <= numWorkers; i++ {
		go func() {
			wg.Add(1)
			defer wg.Done()

			buf := make([]byte, chunkSize)
			fmt.Printf("[worker %d] processing \n", i)
			for offset := range chunkOffsetCh {
				n, err := file.ReadAt(buf, offset+128) //add 128 bytes to ensure covering cut-off lines
				if err != nil {
					break
				}
				buf = buf[:n]

				if n == 0 {
					if err != nil {
						break
					}
				}
				// write result to channel
				chunkMeasurementsCh <- processChunk(buf)
			}
			fmt.Printf("finishing worker %d\n", i)
		}()
	}

	go func() {
		wg.Wait()
		close(chunkMeasurementsCh)
	}()

	mergedResults := make(map[string]*measurement)
	for chunkResult := range chunkMeasurementsCh {
		mergedResults = mergeMeasurements(chunkResult, mergedResults)
	}

	return mergedResults
}

func mergeMeasurements(chunk map[string]*measurement, result map[string]*measurement) map[string]*measurement {
	for id := range chunk {
		m := result[id]

		if m == nil {
			result[id] = chunk[id]
			continue
		}

		if chunk[id].min < m.min {
			m.min = chunk[id].min
		}

		if chunk[id].max > m.max {
			m.max = chunk[id].max
		}

		m.sum += chunk[id].sum
		m.count++

	}
	return result
}

func processChunk(buf []byte) map[string]*measurement {
	results := make(map[string]*measurement)

	var id string
	var temperature float64
	var startID, startValue int

	for idx := 0; idx < len(buf); idx++ {
		if buf[idx] == '\n' {
			if startID == 0 { // We skip the first row due to the buffer added
				startID = idx + 1
				continue
			}

			temperature = parseTemperature(buf[startValue:idx])
			m := results[id]
			if m == nil {
				results[id] = &measurement{
					min:   temperature,
					max:   temperature,
					sum:   temperature,
					count: 1,
				}
			} else {
				if temperature > m.max {
					m.max = temperature
				}
				if temperature < m.min {
					m.min = temperature
				}
				m.sum += temperature
				m.count++
			}
			startID = idx + 1
		}
		if buf[idx] == ';' {
			id = string(buf[startID:idx])
			startValue = idx + 1
		}
	}

	return results
}

func parseTemperature(bs []byte) float64 {
	var intStartIdx int // is negative?
	if bs[0] == '-' {
		intStartIdx = 1
	}

	v := float64(bs[len(bs)-1]-'0') / 10 // single decimal digit
	place := 1.0
	for i := len(bs) - 3; i >= intStartIdx; i-- { // integer part
		v += float64(bs[i]-'0') * place
		place *= 10
	}

	if intStartIdx == 1 {
		v *= -1
	}
	return v
}

// processRow gives returns id
func processRow(row string) (string, float64) {
	elements := strings.Split(row, separator)
	if len(elements) != 2 {
		return "", 0
	}
	temp, err := strconv.ParseFloat(strings.Replace(elements[1], "\n", "", 1), 64)
	if err != nil {
		return "", 0
	}

	return strings.Replace(elements[0], "\n", "", 1), temp
}

func printResults(results map[string]*measurement) {
	var builder strings.Builder

	for name := range results {
		s := results[name]
		avg := math.Floor((s.sum/float64(s.count)+0.05)*10) / 10
		builder.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f", name, s.min, avg, s.max))
		builder.WriteString(", ")
	}

	writer := bufio.NewWriter(os.Stdout)
	fmt.Fprintf(writer, "{%s}\n", builder.String())
	writer.Flush()

}
