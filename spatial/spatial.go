// Copyright (c) 2025 Daniar Achakeev
// This source code is licensed under the MIT license found in the LICENSE.txt file in the root directory.
package spatial

import (
	"fmt"
	"math"
)

// DoublePoint represents a point in N-dimensional space using double-precision floating-point numbers.
type DoublePoint []float64

func (d DoublePoint) Dimension() int {
	return len(d)
}

// DoublePointRectangle represents a rectangle in N-dimensional space using double-precision floating-point numbers.
type DoublePointRectangle struct {
	LowLeft DoublePoint // non private to allow direct access
	UpRight DoublePoint // non private to allow direct access
}

// returns  a pointer to a new DoublePointRectangle
func NewDoublePointRectangle(lowLeft, upRight DoublePoint) (*DoublePointRectangle, error) {
	if lowLeft.Dimension() != upRight.Dimension() {
		return nil, fmt.Errorf("length of lowLeft and upRight must match")
	}
	return &DoublePointRectangle{
		LowLeft: lowLeft,
		UpRight: upRight,
	}, nil
}

func (r *DoublePointRectangle) Dimension() int {
	return r.LowLeft.Dimension()
}

// Union returns a new DoublePointRectangle that is the union of the current rectangle and another rectangle.
func (r *DoublePointRectangle) Union(other *DoublePointRectangle) *DoublePointRectangle {
	if r.Dimension() != other.Dimension() {
		// change to error return?
		panic("Dimensions must match for union operation")
	}
	newLowLeft := make(DoublePoint, r.Dimension())
	newUpRight := make(DoublePoint, r.Dimension())
	for i := 0; i < r.Dimension(); i++ {
		if r.LowLeft[i] < other.LowLeft[i] {
			newLowLeft[i] = r.LowLeft[i]
		} else {
			newLowLeft[i] = other.LowLeft[i]
		}
		if r.UpRight[i] > other.UpRight[i] {
			newUpRight[i] = r.UpRight[i]
		} else {
			newUpRight[i] = other.UpRight[i]
		}
	}
	return &DoublePointRectangle{
		LowLeft: newLowLeft,
		UpRight: newUpRight,
	}
}

func (r *DoublePointRectangle) UnionInPlace(other *DoublePointRectangle) {
	if r.Dimension() != other.Dimension() {
		panic("Dimensions must match for union operation")
	}
	for i := 0; i < r.Dimension(); i++ {
		if r.LowLeft[i] > other.LowLeft[i] {
			r.LowLeft[i] = other.LowLeft[i]
		}
		if r.UpRight[i] < other.UpRight[i] {
			r.UpRight[i] = other.UpRight[i]
		}
	}
}

// Intersects checks if the current rectangle intersects with another rectangle.
func (r *DoublePointRectangle) Intersects(other *DoublePointRectangle) bool {
	if len(r.LowLeft) != len(other.LowLeft) {
		return false
	}
	// negate interval overlap condition
	// ovrelap other.lowLeft[i] <= r.upRight[i] && other.upRight[i] >= r.lowLeft[i]
	// negate   !(other.lowLeft[i] <= r.upRight[i]) || !( other.upRight[i] >= r.lowLeft[i])
	// which is equivalent to
	// other.lowLeft[i] > r.upRight[i] || other.upRight[i] < r.lowLeft[i]
	for i := 0; i < len(r.LowLeft); i++ {
		if other.LowLeft[i] > r.UpRight[i] || other.UpRight[i] < r.LowLeft[i] {
			return false
		}
	}
	return true
}

// Area calculates the area (or hypervolume) of the rectangle.
func (r *DoublePointRectangle) Area() float64 {
	area := 1.0
	dimension := r.Dimension()
	for i := 0; i < dimension; i++ {
		area *= r.UpRight[i] - r.LowLeft[i]
	}
	return area
}

// Clone creates a deep copy of the DoublePointRectangle.
func (r *DoublePointRectangle) Clone() *DoublePointRectangle {
	newLowLeft := make([]float64, r.Dimension())
	newUpRight := make([]float64, r.Dimension())
	copy(newLowLeft, r.LowLeft)
	copy(newUpRight, r.UpRight)
	return &DoublePointRectangle{
		LowLeft: newLowLeft,
		UpRight: newUpRight,
	}
}

// Center calculates the center point of the rectangle.
func (r *DoublePointRectangle) Center() DoublePoint {
	center := make(DoublePoint, r.Dimension())
	for i := 0; i < r.Dimension(); i++ {
		center[i] = (r.LowLeft[i] + r.UpRight[i]) / 2
	}
	return center
}

func (r *DoublePointRectangle) Equals(other *DoublePointRectangle) bool {
	if r.Dimension() != other.Dimension() {
		return false
	}
	for i := 0; i < r.Dimension(); i++ {
		if r.LowLeft[i] != other.LowLeft[i] || r.UpRight[i] != other.UpRight[i] {
			return false
		}
	}
	return true
}

func (r *DoublePointRectangle) String() string {
	result := "DoublePointRectangle{"
	result += "lowLeft: ["
	for i, val := range r.LowLeft {
		if i > 0 {
			result += ", "
		}
		result += fmt.Sprintf("%f", val)
	}
	result += "], "
	result += "upRight: ["
	for i, val := range r.UpRight {
		if i > 0 {
			result += ", "
		}
		result += fmt.Sprintf("%f", val)
	}
	result += "]"
	result += "}"
	return result
}

const Resolution2D8 = 8
const Resolution2D16 = 16
const Resolution2D21 = 21
const Resolution2D32 = 32
const NormF8 = 255.0
const NormF16 = 65535.0
const NormF21 = 2097151.0
const NormF32 = 4294967295.0

func GetPeanoCurveValue2D32(point DoublePoint, globalRect *DoublePointRectangle) uint64 {
	// normalize
	var x, y uint64
	var peanoValueZ uint64
	wx := globalRect.UpRight[0] - globalRect.LowLeft[0]
	wy := globalRect.UpRight[1] - globalRect.LowLeft[1]
	if wx != 0.0 {
		x = uint64(math.Round(math.Max(0.0, math.Min(1.0, (point[0]-globalRect.LowLeft[0])/wx)) * NormF32))
	}
	if wy != 0.0 {
		y = uint64(math.Round(math.Max(0.0, math.Min(1.0, (point[1]-globalRect.LowLeft[1])/wy)) * NormF32))
	}
	for i := 0; i < Resolution2D32; i++ {
		// shift scanner
		bitX := (x >> i) & 1
		bitY := (y >> i) & 1
		xy := (bitY << 1) | bitX
		peanoValueZ |= (xy << uint(2*i))
	}
	return peanoValueZ
}

func GetPeanoCurveValue2D16(point DoublePoint, globalRect *DoublePointRectangle) uint64 {
	// normalize
	var x, y uint64
	var peanoValueZ uint64
	wx := globalRect.UpRight[0] - globalRect.LowLeft[0]
	wy := globalRect.UpRight[1] - globalRect.LowLeft[1]
	if wx != 0.0 {
		x = uint64(math.Round(math.Max(0.0, math.Min(1.0, (point[0]-globalRect.LowLeft[0])/wx)) * NormF16))
	}
	if wy != 0.0 {
		y = uint64(math.Round(math.Max(0.0, math.Min(1.0, (point[1]-globalRect.LowLeft[1])/wy)) * NormF16))
	}
	for i := 0; i < Resolution2D16; i++ {
		// shift scanner
		bitX := (x >> i) & 1
		bitY := (y >> i) & 1
		xy := (bitY << 1) | bitX
		peanoValueZ |= (xy << uint(2*i))
	}
	return peanoValueZ
}
