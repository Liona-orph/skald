package execution

import "math"

// splitmix64 is a fast, well-distributed mixing function. Skald uses it instead
// of math/rand for every value that ends up in a history event, because the
// engine needs randomness that is *reproducible from data already written down*.
//
// Given a run's seed and a counter, any replica of the engine derives the same
// value without coordinating, which is what lets a retry backoff be recomputed
// during replay rather than remembered.
func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	z := x
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// jitterFromSeed derives a value in [0,1) from a run seed and a counter.
func jitterFromSeed(seed, counter int64) float64 {
	v := splitmix64(uint64(seed) ^ splitmix64(uint64(counter)))
	// Use the top 53 bits so the result maps exactly onto float64 mantissa
	// precision, avoiding the subtle non-uniformity of a plain modulo.
	return float64(v>>11) / float64(uint64(1)<<53)
}

// nextSeed derives the seed for a successor run.
func nextSeed(seed int64) int64 {
	return int64(splitmix64(uint64(seed)) & math.MaxInt64)
}
