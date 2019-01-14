// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package exec

import (
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/types"
)

func TestStep(t *testing.T) {
	tcs := []struct {
		step     int
		tuples   []tuple
		expected []tuple
	}{
		{
			step:     1,
			tuples:   tuples{{1}, {2}, {3}, {4}},
			expected: tuples{{1}, {2}, {3}, {4}},
		},
		{
			step:     2,
			tuples:   tuples{{1}, {2}, {3}, {4}},
			expected: tuples{{1}, {3}},
		},
		{
			step:     100000,
			tuples:   tuples{{1}, {2}, {3}, {4}},
			expected: tuples{{1}},
		},
	}

	for _, tc := range tcs {
		runTests(t, []tuples{tc.tuples}, []types.T{}, func(t *testing.T, input []Operator) {
			s := NewStepOp(input[0], tc.step)
			out := newOpTestOutput(s, []int{0}, tc.expected)

			if err := out.Verify(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func BenchmarkStep(b *testing.B) {
	rng, _ := randutil.NewPseudoRand()

	batch := NewMemBatch([]types.T{types.Int64, types.Int64, types.Int64})
	aCol := batch.ColVec(1).Int64()
	bCol := batch.ColVec(2).Int64()
	lastA := int64(0)
	lastB := int64(0)
	for i := 0; i < ColBatchSize; i++ {
		// 1/4 chance of changing each distinct col.
		if rng.Float64() > 0.75 {
			lastA++
		}
		if rng.Float64() > 0.75 {
			lastB++
		}
		aCol[i] = lastA
		bCol[i] = lastB
	}
	batch.SetLength(ColBatchSize)
	source := newRepeatableBatchSource(batch)
	source.Init()

	distinct := NewStepOp(source, 2)

	// don't count the artificial zeroOp'd column in the throughput
	b.SetBytes(int64(8 * ColBatchSize * 3))
	for i := 0; i < b.N; i++ {
		distinct.Next()
	}
}
