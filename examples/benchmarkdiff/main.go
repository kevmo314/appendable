package main

import (
	"bufio"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/components"
	"github.com/go-echarts/go-echarts/v2/opts"
)

type record struct {
	timestamp int
	n         int
	size      int
}

func readFile(f *os.File) ([]record, error) {
	// read the file and parse the (timestamp,n,size) tuples
	s := bufio.NewScanner(f)
	var records []record
	for s.Scan() {
		// parse the line
		line := s.Text()
		// split the line
		tokens := strings.Split(line, ",")
		// convert the tokens to integers
		timestamp, err := strconv.Atoi(tokens[0])
		if err != nil {
			return nil, err
		}
		n, err := strconv.Atoi(tokens[1])
		if err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(tokens[2])
		if err != nil {
			return nil, err
		}
		records = append(records, record{timestamp, n, size})
	}
	return records, s.Err()
}

func generateXAxis(records []record) []int {
	var xAxis []int
	for _, r := range records {
		xAxis = append(xAxis, r.n)
	}
	return xAxis
}

func generateTimestampYAxis(records []record) []opts.LineData {
	var yAxis []opts.LineData
	for _, r := range records {
		yAxis = append(yAxis, opts.LineData{Value: r.timestamp})
	}
	return yAxis
}

func generateSizeYAxis(records []record) []opts.LineData {
	var yAxis []opts.LineData
	for _, r := range records {
		yAxis = append(yAxis, opts.LineData{Value: r.size})
	}
	return yAxis
}

func generateTimestampDeltaYAxis(r1, r2 []record) []opts.LineData {
	var yAxis []opts.LineData
	for i := range r1 {
		yAxis = append(yAxis, opts.LineData{Value: r2[i].timestamp - r1[i].timestamp})
	}
	return yAxis
}

func generateSizeDeltaYAxis(r1, r2 []record) []opts.LineData {
	var yAxis []opts.LineData
	for i := range r1 {
		yAxis = append(yAxis, opts.LineData{Value: r2[i].size - r1[i].size})
	}
	return yAxis
}

func main() {
	// read two arguments as files and parse the (timestamp,n,size) tuples
	f1, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer f1.Close()
	records1, err := readFile(f1)
	if err != nil {
		panic(err)
	}
	f2, err := os.Open(os.Args[2])
	if err != nil {
		panic(err)
	}
	defer f2.Close()
	records2, err := readFile(f2)
	if err != nil {
		panic(err)
	}

	// generate four charts:
	// 1. timestamp vs n
	// 2. pagefile size vs n
	// 3. timestamp delta vs n
	// 4. pagefile size delta vs n

	line1 := charts.NewLine()
	line1.SetGlobalOptions(
		charts.WithTooltipOpts(opts.Tooltip{Show: true, Trigger: "axis"}),
		charts.WithYAxisOpts(opts.YAxis{
			Name: "Time (μs)",
		}),
		charts.WithXAxisOpts(opts.XAxis{
			Name: "Bytes read",
		}))
	line1.SetXAxis(generateXAxis(records1)).
		AddSeries("Run 1", generateTimestampYAxis(records1)).
		AddSeries("Run 2", generateTimestampYAxis(records2))

	line2 := charts.NewLine()
	line2.SetGlobalOptions(
		charts.WithTooltipOpts(opts.Tooltip{Show: true, Trigger: "axis"}),
		charts.WithYAxisOpts(opts.YAxis{
			Name: "Size (pages)",
		}),
		charts.WithXAxisOpts(opts.XAxis{
			Name: "Bytes read",
		}))
	line2.SetXAxis(generateXAxis(records1)).
		AddSeries("Run 1", generateSizeYAxis(records1)).
		AddSeries("Run 2", generateSizeYAxis(records2))

	line3 := charts.NewLine()
	line3.SetGlobalOptions(
		charts.WithYAxisOpts(opts.YAxis{
			Name: "Time delta (μs)",
		}),
		charts.WithXAxisOpts(opts.XAxis{
			Name: "Bytes read",
		}))
	line3.SetXAxis(generateXAxis(records1)).
		AddSeries("Time delta", generateTimestampDeltaYAxis(records1, records2))

	line4 := charts.NewLine()
	line4.SetGlobalOptions(
		charts.WithYAxisOpts(opts.YAxis{
			Name: "Size delta (pages)",
		}),
		charts.WithXAxisOpts(opts.XAxis{
			Name: "Bytes read",
		}))
	line4.SetXAxis(generateXAxis(records1)).
		AddSeries("Size delta", generateSizeDeltaYAxis(records1, records2))

	page := components.NewPage()
	page.PageTitle = "Benchmark diff"
	page.AddCharts(
		line1,
		line2,
		line3,
		line4,
	)
	f, err := os.Create("output.html")
	if err != nil {
		panic(err)
	}
	page.Render(io.MultiWriter(f))
}
