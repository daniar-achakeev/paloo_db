package spatial

import (
	"encoding/xml"
	"fmt"
	"math"
	"os"
	"slices"
	"testing"
)

// represents the <rect> element.
type Rectangle struct {
	XMLName     xml.Name `xml:"rect"`
	X           string   `xml:"x,attr"`
	Y           string   `xml:"y,attr"`
	Width       string   `xml:"width,attr"`
	Height      string   `xml:"height,attr"`
	Fill        string   `xml:"fill,attr"`
	Stroke      string   `xml:"stroke,attr"`
	StrokeWidth string   `xml:"stroke-width,attr"`
}

type Svg struct {
	XMLName xml.Name    `xml:"svg"`
	Width   string      `xml:"width,attr"`
	Height  string      `xml:"height,attr"`
	ViewBox string      `xml:"viewBox,attr"`
	Xmlns   string      `xml:"xmlns,attr"`
	Rects   []Rectangle `xml:"rect"`
}

func (s Svg) writeSvg(path string) error {
	mBytes, error := xml.MarshalIndent(s, "", "  ")
	if error != nil {
		return fmt.Errorf("error marshalling svg: %v", error)
	}
	xmlOutput := []byte(xml.Header + string(mBytes))
	error = os.WriteFile(path, xmlOutput, 0644)
	if error != nil {
		return fmt.Errorf("error writing file: %v", error)
	}
	return nil
}

const D2 = 2 // two dimensions

// Writes
type SVGPainter struct {
	height        int
	width         int
	svgRectangles []Rectangle
	length        [D2]float64
	uni           DoublePointRectangle
}

func NewSVGPainter(width, height int, uni DoublePointRectangle) *SVGPainter {
	length := [D2]float64{0.0, 0.0}
	for i := range 2 {
		length[i] = uni.UpRight[i] - uni.LowLeft[i]
	}
	svgRectangles := make([]Rectangle, 0, 256)
	return &SVGPainter{
		height:        height,
		width:         width,
		svgRectangles: svgRectangles,
		length:        length,
		uni:           uni,
	}
}

func (p *SVGPainter) AddRectangle(rec DoublePointRectangle, stroke string, strokeWidth string) {
	lP := make([]int, 2)
	rP := make([]int, 2)
	lP[0] = int(math.Round(math.Max(0.0, math.Min(1.0, (rec.LowLeft[0]-p.uni.LowLeft[0])/p.length[0])) * float64(p.width)))
	lP[1] = int(math.Round(math.Max(0.0, math.Min(1.0, (rec.LowLeft[1]-p.uni.LowLeft[1])/p.length[1])) * float64(p.height)))
	rP[0] = int(math.Round(math.Max(0.0, math.Min(1.0, (rec.UpRight[0]-p.uni.LowLeft[0])/p.length[0])) * float64(p.width)))
	rP[1] = int(math.Round(math.Max(0.0, math.Min(1.0, (rec.UpRight[1]-p.uni.LowLeft[1])/p.length[1])) * float64(p.height)))
	width := rP[0] - lP[0]
	if width == 0 {
		width = 1 // for points
	}
	height := rP[1] - lP[1]
	if height == 0 {
		height = 1
	}
	svgRect := Rectangle{
		X:           fmt.Sprintf("%d", lP[0]),
		Y:           fmt.Sprintf("%d", p.height-lP[1]-height),
		Width:       fmt.Sprintf("%d", width),
		Height:      fmt.Sprintf("%d", height),
		Fill:        "none",
		Stroke:      stroke,
		StrokeWidth: strokeWidth,
	}
	p.svgRectangles = append(p.svgRectangles, svgRect)
}

func (p *SVGPainter) SaveToFile(path string) error {
	canvas := Svg{
		Width:   fmt.Sprintf("%d", p.width),
		Height:  fmt.Sprintf("%d", p.height),
		ViewBox: fmt.Sprintf("%d %d %d %d", 0, 0, p.width+2, p.height+2),
		Xmlns:   "http://www.w3.org/2000/svg",
		Rects:   p.svgRectangles,
	}
	return canvas.writeSvg(path)
}

func TestDoublePointRectangleIntersectTest(t *testing.T) {
	r1, _ := NewDoublePointRectangle([]float64{2, 2}, []float64{6, 6})
	r2, _ := NewDoublePointRectangle([]float64{4, 4}, []float64{10, 10})
	r3, _ := NewDoublePointRectangle([]float64{2, 2}, []float64{10, 10})
	r4, _ := NewDoublePointRectangle([]float64{4, 4}, []float64{6, 6})
	r5, _ := NewDoublePointRectangle([]float64{5, 5}, []float64{8, 8})
	r6, _ := NewDoublePointRectangle([]float64{2, 10}, []float64{6, 12})

	tests := []struct {
		name     string
		rect1    *DoublePointRectangle
		rect2    *DoublePointRectangle
		expected bool
	}{
		{"left_lower",
			r1, r2,
			true,
		},
		{"inside",
			r3, r4,
			true,
		},
		{"inside",
			r5, r6,
			false,
		},
	}
	for _, tt := range tests {
		t.Logf("Running test: %s", tt.name)
		t.Run(tt.name, func(t *testing.T) {
			result := tt.rect1.Intersects(*tt.rect2)
			if result != tt.expected {
				t.Errorf("Expected %v, but got %v", tt.expected, result)
			}
		})
	}
}

func TestGetPeanoCurveValue2D32(t *testing.T) {
	globalRect, _ := NewDoublePointRectangle([]float64{0, 0}, []float64{1 << 32, 1 << 32})
	point1 := DoublePoint{10, 5}
	value1 := GetPeanoCurveValue2D32(point1, globalRect)
	t.Logf("Peano curve value for point1: %d", value1)
	if value1 != 102 {
		t.Errorf("Expected Peano curve value 102, but got %d", value1)
	}
	point2 := DoublePoint{1, 2}
	value2 := GetPeanoCurveValue2D32(point2, globalRect)
	t.Logf("Peano curve value for point2: %d", value2)
	if value2 != 9 {
		t.Errorf("Expected Peano curve value 9, but got %d", value2)
	}
	point3 := DoublePoint{3, 3}
	value3 := GetPeanoCurveValue2D32(point3, globalRect)
	t.Logf("Peano curve value for point3: %d", value3)
	if value3 != 15 {
		t.Errorf("Expected Peano curve value 15, but got %d", value3)
	}
	point4 := DoublePoint{7, 3}
	value4 := GetPeanoCurveValue2D32(point4, globalRect)
	t.Logf("Peano curve value for point4: %d", value4)
	if value4 != 31 {
		t.Errorf("Expected Peano curve value 31, but got %d", value4)
	}
	point5 := DoublePoint{0, 4}
	value5 := GetPeanoCurveValue2D32(point5, globalRect)
	t.Logf("Peano curve value for point5: %d", value5)
	if value5 != 32 {
		t.Errorf("Expected Peano curve value 32, but got %d", value5)
	}
}

// reuturns rectangles and expected z-order
func getTestPeanoOrderRectangles() ([]DoublePointRectangle, []DoublePointRectangle) {
	r1, _ := NewDoublePointRectangle([]float64{0, 0}, []float64{0, 0})
	r2, _ := NewDoublePointRectangle([]float64{3, 1}, []float64{3, 1})
	r3, _ := NewDoublePointRectangle([]float64{0, 2}, []float64{0, 2})
	r4, _ := NewDoublePointRectangle([]float64{0, 3}, []float64{0, 3})
	r5, _ := NewDoublePointRectangle([]float64{2, 3}, []float64{2, 3})
	r6, _ := NewDoublePointRectangle([]float64{6, 0}, []float64{6, 0})
	r7, _ := NewDoublePointRectangle([]float64{7, 2}, []float64{7, 2})
	r8, _ := NewDoublePointRectangle([]float64{0, 5}, []float64{0, 5})
	r9, _ := NewDoublePointRectangle([]float64{2, 5}, []float64{2, 5})
	r10, _ := NewDoublePointRectangle([]float64{0, 7}, []float64{0, 7})
	r11, _ := NewDoublePointRectangle([]float64{7, 4}, []float64{7, 4})
	r12, _ := NewDoublePointRectangle([]float64{7, 5}, []float64{7, 5})
	r13, _ := NewDoublePointRectangle([]float64{4, 6}, []float64{4, 6})
	r14, _ := NewDoublePointRectangle([]float64{7, 7}, []float64{7, 7})
	// expected
	expectedRectangles := []DoublePointRectangle{*r1, *r2, *r3, *r4, *r5, *r6, *r7, *r8, *r9, *r10, *r11, *r12, *r13, *r14}
	rectangles := []DoublePointRectangle{*r14, *r12, *r13, *r11, *r10, *r9, *r8, *r7, *r6, *r5, *r4, *r3, *r2, *r1}
	return rectangles, expectedRectangles
}

func TestComputeUniverse(t *testing.T) {
	rectangles, expectedRectangles := getTestPeanoOrderRectangles()
	u1 := ComputeUniverse(slices.Values(rectangles))
	u2 := ComputeUniverse(slices.Values(expectedRectangles))
	if !u1.Equals(u2) {
		t.Fatal("same sets should have the same universe ")
	}
	t.Log("U1", u1, "U2", u2)
	expectedUniverse := DoublePointRectangle{
		LowLeft: []float64{0, 0},
		UpRight: []float64{7, 7},
	}
	if !u1.Equals(expectedUniverse) {
		t.Fatalf(" Expected %s got %s ", &u1, &expectedUniverse)
	}
}

func TestPaintRectangles(t *testing.T) {
	_, expectedRectangles := getTestPeanoOrderRectangles()
	uni := ComputeUniverse(slices.Values(expectedRectangles))
	painter := NewSVGPainter(500, 500, uni)
	for rec := range slices.Values(expectedRectangles) {
		painter.AddRectangle(rec, "blue", "2")
	}
	file := "./rectangles.svg"
	painter.SaveToFile(file)
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatalf("error reading SVG file: %v", err)
	}
	var svgData Svg
	err = xml.Unmarshal(data, &svgData)
	if err != nil {
		t.Fatalf("error unmarshalling SVG data: %v", err)
	}
	if len(svgData.Rects) != len(expectedRectangles) {
		t.Fatalf("expected %d rectangles, but got %d", len(expectedRectangles), len(svgData.Rects))
	}

}

func TestGOPTPartitioning(t *testing.T) {
	rectangles, expectedRectangles := getTestPeanoOrderRectangles()
	globalRect, _ := NewDoublePointRectangle([]float64{0, 0}, []float64{1 << 32, 1 << 32})
	for _, r := range rectangles {
		center := r.Center()
		left := r.LowLeft
		if !slices.Equal(center, left) {
			t.Errorf("Center function %v", r)
		}
	}
	slices.SortFunc(rectangles, func(l DoublePointRectangle, r DoublePointRectangle) int {
		centerL := l.Center()
		centerR := r.Center()
		valueL := GetPeanoCurveValue2D32(centerL, globalRect)
		valueR := GetPeanoCurveValue2D32(centerR, globalRect)
		return int(valueL) - int(valueR)
	})
	if !slices.EqualFunc(rectangles, expectedRectangles, func(l DoublePointRectangle, r DoublePointRectangle) bool {
		return l.Equals(r)
	}) {
		t.Errorf("Expected %v, but got %v", expectedRectangles, rectangles)
	}
	b, B := 2, 4
	costs := GOPTPartitions(rectangles, b, B)
	for idx, bucket := range costs {
		t.Log("Bucket", bucket, idx)
	}
	partitions := BackTraceGopt(rectangles, costs, b)
	t.Log(partitions)
	// write svg
	uni := ComputeUniverse(slices.Values(expectedRectangles))
	painter := NewSVGPainter(500, 500, uni)
	for rec := range slices.Values(expectedRectangles) {
		painter.AddRectangle(rec, "blue", "2")
	}
	for _, p := range partitions {
		painter.AddRectangle(p.Mbr, "red", "1")
	}
	file := "./rectangles.svg"
	painter.SaveToFile(file)
}
