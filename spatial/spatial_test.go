package spatial

import (
	"slices"
	"testing"
)

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

func TestGOPTPartitioning(t *testing.T) {
	r1, _ := NewDoublePointRectangle([]float64{0, 0}, []float64{0, 0})
	r2, _ := NewDoublePointRectangle([]float64{3, 0}, []float64{3, 0})
	r3, _ := NewDoublePointRectangle([]float64{2, 1}, []float64{2, 1})
	r4, _ := NewDoublePointRectangle([]float64{0, 2}, []float64{0, 2})
	r5, _ := NewDoublePointRectangle([]float64{2, 3}, []float64{2, 3})
	r6, _ := NewDoublePointRectangle([]float64{5, 0}, []float64{5, 0})
	r7, _ := NewDoublePointRectangle([]float64{7, 2}, []float64{7, 2})
	r8, _ := NewDoublePointRectangle([]float64{0, 5}, []float64{0, 5})
	r9, _ := NewDoublePointRectangle([]float64{0, 7}, []float64{0, 7})
	r10, _ := NewDoublePointRectangle([]float64{7, 4}, []float64{7, 4})
	r11, _ := NewDoublePointRectangle([]float64{7, 5}, []float64{7, 5})
	r12, _ := NewDoublePointRectangle([]float64{4, 6}, []float64{4, 6})
	r13, _ := NewDoublePointRectangle([]float64{7, 7}, []float64{7, 7})
	// expected
	expectedRectangles := []DoublePointRectangle{*r1, *r2, *r3, *r4, *r5, *r6, *r7, *r8, *r9, *r10, *r11, *r12, *r13}
	rectangles := []DoublePointRectangle{*r12, *r13, *r11, *r10, *r9, *r8, *r7, *r6, *r5, *r4, *r3, *r2, *r1}
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
		return l.Equals(&r)
	}) {
		t.Errorf("Expected %v, but got %v", expectedRectangles, rectangles)
	}
	b, B := 2, 4
	costs := GOPTPartitions(rectangles, b, B)
	for idx, bucket := range costs {
		t.Log("Bucket", bucket, idx)
	}

}
