// Copyright (c) 2025 Daniar Achakeev
// This source code is licensed under the MIT license found in the LICENSE.txt file in the root directory.
package spatial

import (
	"fmt"
	"iter"
	"math"
)

const Resolution2D8 = 8
const Resolution2D16 = 16
const Resolution2D21 = 21
const Resolution2D32 = 32
const NormF8 = 255.0
const NormF16 = 65535.0
const NormF21 = 2097151.0
const NormF32 = 4294967295.0

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

func (r *DoublePointRectangle) Union(other DoublePointRectangle) {
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
func (r *DoublePointRectangle) Intersects(other DoublePointRectangle) bool {
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

// HyperVolume calculates the area if 2D (or hypervolume) of the rectangle.
func (r *DoublePointRectangle) HyperVolume() float64 {
	area := 1.0
	dimension := r.Dimension()
	for i := 0; i < dimension; i++ {
		area *= r.UpRight[i] - r.LowLeft[i]
	}
	return area
}

// Clone creates a deep copy of the DoublePointRectangle.
func (r *DoublePointRectangle) Clone() DoublePointRectangle {
	newLowLeft := make([]float64, r.Dimension())
	newUpRight := make([]float64, r.Dimension())
	copy(newLowLeft, r.LowLeft)
	copy(newUpRight, r.UpRight)
	return DoublePointRectangle{
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

func (r *DoublePointRectangle) Equals(other DoublePointRectangle) bool {
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

// GetPeanoCurveValue2D32 : computes 32 bit 2D peano bit interleaving
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

// GetPeanoCurveValue2D16 16 Bit Z-Curve
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

// Bucket is internal sturct to hold cost and prev index
type Bucket struct {
	R         DoublePointRectangle // union of the rectangle
	COpt      float64              // current sum opt value
	prevIndex int                  // index of the previous bucket
}

// MbrPartition holds minimal bounding rectangle and
type MbrPartition struct {
	Mbr       DoublePointRectangle   // minimal bounding rectangle
	Partition []DoublePointRectangle // rectangle inside MBR
}

func (p MbrPartition) String() string {
	return fmt.Sprintf("MBR %s partition %v ", &p.Mbr, p.Partition)
}

// GoptHyperVolume computes an gopt() function on a set of rectangles
// using dynamic programming see Achakeev, Seeger, Widmayer et. Al.
// "Sort-based query-adaptive loading of R-trees" CIKM 2012
// we minimize sum of hypervolumes
func GOPTPartitions(rectangles []DoublePointRectangle, b int, B int) []Bucket {
	bufLen := B - b + 1
	buffer := make([]DoublePointRectangle, bufLen)
	costs := make([]Bucket, len(rectangles))
	// initialize until index b-1
	for i := 0; i < b-1; i++ {
		costs[i].COpt = math.MaxFloat64
	}
	// main loop
	for i := b - 1; i < len(rectangles); i++ {
		// sub routine for readability
		precomputeRectangles(rectangles, buffer, i, b, B)
		// now go back to find the arg min
		costs[i].COpt = math.MaxFloat64
		// old style looping
		for j := 0; j < bufLen; j++ {
			// now pos 0 is equal to i - B+ 1 there is the rectangle of size B and its area
			prevIdx := i - B + j
			// specialCase
			if prevIdx == -1 {
				costs[i].COpt = buffer[j].HyperVolume()
				costs[i].R = buffer[j].Clone()
				costs[i].prevIndex = prevIdx
				continue
			}
			if prevIdx >= 0 {
				// check costs only if there are not max
				// all indexes < b-1 are max
				prevCost := costs[prevIdx].COpt
				if prevCost != math.MaxFloat64 {
					prevCost += buffer[j].HyperVolume()
					if prevCost < costs[i].COpt {
						costs[i].COpt = prevCost
						costs[i].R = buffer[j].Clone()
						costs[i].prevIndex = prevIdx
					}
				}
			}
		}
	}
	return costs
}

// precomputeVolumes function runs from position t to t-B to compute b till B sized volumes
// subfunction mutates the [B-b+1] length
func precomputeRectangles(sourceSlice []DoublePointRectangle, buffeSlice []DoublePointRectangle, startIndex int, b int, B int) {
	// take rectangle on the current position
	// assertion .(T)
	var universe DoublePointRectangle
	sj := len(buffeSlice) - 1
	// so now we go backwards and call union
	for i := 0; i < B; i++ {
		nextIdx := startIndex - i
		if nextIdx < 0 {
			break
		}
		if i == 0 {
			universe = sourceSlice[nextIdx].Clone()
		} else {
			universe.Union(sourceSlice[nextIdx].Clone())
		}
		if i+1 >= b {
			buffeSlice[sj] = universe.Clone()
			sj--
		}
	}
}

// DevisePartitioning from cost array and source partitioning
func DevisePartitioning(rectangles []DoublePointRectangle, buckets []Bucket, b int) []MbrPartition {
	idx := len(buckets) - 1
	partitions := make([]MbrPartition, 0, (idx+1)/b) // prealloacate with max cap
	for {
		currentBucket := buckets[idx]
		prevIdx := currentBucket.prevIndex
		mbrPartition := MbrPartition{
			Mbr:       currentBucket.R,
			Partition: rectangles[prevIdx+1 : idx+1], // inclusive
		}
		partitions = append(partitions, mbrPartition)
		idx = prevIdx
		if prevIdx < 0 {
			break
		}
	}
	return partitions

}

// ComputeUniverse returns MBR of the iterator
func ComputeUniverse(recIter iter.Seq[DoublePointRectangle]) DoublePointRectangle {
	head := true
	universe := DoublePointRectangle{}
	for rec := range recIter {
		if head {
			universe = rec.Clone()
			head = false
			continue
		}
		universe.Union(rec)
	}
	return universe
}
