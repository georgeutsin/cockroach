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

// limitOp is an operator that implements limit, returning only the first n
// tuples from its input.
type stepOp struct {
	input Operator

	internalBatch ColBatch

	step     int
	stepLeft int
}

// NewLimitOp returns a new limit operator with the given limit.
func NewStepOp(input Operator, step int) Operator {
	c := &stepOp{
		input:    input,
		step:     step,
		stepLeft: 0,
	}
	return c
}

func (c *stepOp) Init() {
	c.internalBatch = NewMemBatch(nil)
	c.input.Init()
}

func (c *stepOp) Next() ColBatch {
	for {
		bat := c.input.Next()
		length := bat.Length()
		if length == 0 {
			return bat
		}

		sel := bat.Selection()
		count := uint16(0)

		if sel != nil {
			idx := 0
			i := c.stepLeft
			for ; uint16(i) < length; i += c.step {
				sel[idx] = sel[i]
				idx += 1
				count += 1
			}
			c.stepLeft = i - int(length)
		} else {
			bat.SetSelection(true)
			sel := bat.Selection()
			i := c.stepLeft
			idx := 0
			for ; uint16(i) < length; i += c.step {
				sel[idx] = uint16(i)
				count += 1
				idx += 1
			}
			c.stepLeft = i - int(length)
		}

		if count > 0 {
			bat.SetLength(count)

			return bat
		}
	}
}
