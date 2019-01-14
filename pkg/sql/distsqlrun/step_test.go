// Copyright 2016 The Cockroach Authors.
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

package distsqlrun

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestStep(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := [15]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT},
			tree.NewDInt(tree.DInt(i)))
	}

	testCases := []struct {
		step     int
		input    sqlbase.EncDatumRows
		expected sqlbase.EncDatumRows
	}{
		{
			step: 2,
			input: sqlbase.EncDatumRows{
				{v[1], v[2]},
				{v[3], v[4]},
				{v[5], v[6]},
				{v[7], v[8]},
				{v[9], v[10]},
				{v[11], v[12]},
				{v[13], v[14]},
			},
			expected: sqlbase.EncDatumRows{
				{v[1], v[2]},
				{v[5], v[6]},
				{v[9], v[10]},
				{v[13], v[14]},
			},
		},
		{
			step: 3,
			input: sqlbase.EncDatumRows{
				{v[1], v[2]},
				{v[3], v[4]},
				{v[5], v[6]},
				{v[7], v[8]},
				{v[9], v[10]},
				{v[11], v[12]},
				{v[13], v[14]},
			},
			expected: sqlbase.EncDatumRows{
				{v[1], v[2]},
				{v[7], v[8]},
				{v[13], v[14]},
			},
		},
		{
			step: 100,
			input: sqlbase.EncDatumRows{
				{v[1], v[2]},
				{v[3], v[4]},
				{v[5], v[6]},
				{v[7], v[8]},
				{v[9], v[10]},
				{v[11], v[12]},
				{v[13], v[14]},
			},
			expected: sqlbase.EncDatumRows{
				{v[1], v[2]},
			},
		},
		{
			step:     2,
			input:    sqlbase.EncDatumRows{},
			expected: sqlbase.EncDatumRows{},
		},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {

			in := NewRowBuffer(twoIntCols, c.input, RowBufferArgs{})
			out := &RowBuffer{}

			st := cluster.MakeTestingClusterSettings()
			evalCtx := tree.MakeTestingEvalContext(st)
			defer evalCtx.Stop(context.Background())
			flowCtx := FlowCtx{
				Settings: st,
				EvalCtx:  &evalCtx,
			}

			d, err := newStepProcessor(&flowCtx, 0 /* processorID */, in, &distsqlpb.PostProcessSpec{}, out, c.step)
			if err != nil {
				t.Fatal(err)
			}

			d.Run(context.Background())
			if !out.ProducerClosed {
				t.Fatalf("output RowReceiver not closed")
			}
			var res sqlbase.EncDatumRows
			for {
				row := out.NextNoMeta(t).Copy()
				if row == nil {
					break
				}
				res = append(res, row)
			}

			if result := res.String(twoIntCols); result != c.expected.String(twoIntCols) {
				t.Errorf("invalid results: %s, expected %s'", result, c.expected.String(twoIntCols))
			}
		})
	}
}

func BenchmarkStep(b *testing.B) {
	const numRows = 1 << 16

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := &FlowCtx{
		Settings: st,
		EvalCtx:  &evalCtx,
	}
	post := &distsqlpb.PostProcessSpec{}
	disposer := &RowDisposer{}
	for _, numCols := range []int{1, 1 << 1, 1 << 2, 1 << 4, 1 << 8} {
		b.Run(fmt.Sprintf("cols=%d", numCols), func(b *testing.B) {
			cols := make([]sqlbase.ColumnType, numCols)
			for i := range cols {
				cols[i] = intType
			}
			input := NewRepeatableRowSource(cols, makeIntRows(numRows, numCols))

			b.SetBytes(int64(8 * numRows * numCols))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				d, err := newStepProcessor(flowCtx, 0 /* processorID */, input, post, disposer, 2)
				if err != nil {
					b.Fatal(err)
				}
				d.Run(context.Background())
				input.Reset()
			}
		})
	}
}
