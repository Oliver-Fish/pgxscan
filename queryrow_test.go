package pgxscan

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func stringPtr(s string) *string {
	return &s
}

func pointInTimePtr(t *testing.T) *time.Time {
	timeStamp, err := time.Parse(time.RFC3339, "2006-01-02T15:04:05Z")
	require.NoError(t, err)
	return &timeStamp
}

func intPtr(i int) *int {
	return &i
}

func TestBasicStructQueryRowScan(t *testing.T) {
	type testStruct struct {
		A string     `db:"a"`
		B *string    `db:"b"`
		C time.Time  `db:"c"`
		D *time.Time `db:"d"`
		E int        `db:"e"`
		F *int       `db:"f"`
	}

	tests := map[string]struct {
		query         string
		expected      testStruct
		expectedError error
	}{
		"All Properties": {
			query: `
			SELECT
				'somethingA' as a,
				'somethingB' as b,
				TIMESTAMPTZ '2006-01-02T15:04:05Z' as c,
				TIMESTAMPTZ '2006-01-02T15:04:05Z' as d,
				1 as e,
				1 as f
			`,
			expected: testStruct{
				A: "somethingA",
				B: stringPtr("somethingB"),
				C: *pointInTimePtr(t),
				D: pointInTimePtr(t),
				E: 1,
				F: intPtr(1),
			},
		},
		"Only Pointers": {
			query: `
			SELECT
				'somethingB' as b,
				TIMESTAMPTZ '2006-01-02T15:04:05Z' as d,
				1 as f
			`,
			expected: testStruct{
				A: "",
				B: stringPtr("somethingB"),
				C: time.Time{},
				D: pointInTimePtr(t),
				E: 0,
				F: intPtr(1),
			},
			expectedError: ErrQueryColumnsTagsMismtach,
		},
		"Only Values": {
			query: `
			SELECT
				'somethingA' as a,
				TIMESTAMPTZ '2006-01-02T15:04:05Z' as c,
				1 as e
			`,
			expected: testStruct{
				A: "somethingA",
				B: nil,
				C: *pointInTimePtr(t),
				D: nil,
				E: 1,
				F: nil,
			},
			expectedError: ErrQueryColumnsTagsMismtach,
		},
		"Pointer Values set to NULL": {
			query: `
			SELECT
				'somethingA' as a,
				NULL as b,
				TIMESTAMPTZ '2006-01-02T15:04:05Z' as c,
				NULL as d,
				1 as e,
				NULL as f
			`,
			expected: testStruct{
				A: "somethingA",
				B: nil,
				C: *pointInTimePtr(t),
				D: nil,
				E: 1,
				F: nil,
			},
		},
	}

	ctx := context.Background()
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var val testStruct
			err := QueryRow(ctx, db, &val, tc.query)
			require.Equal(t, tc.expectedError, err)
			require.Equal(t, tc.expected.A, val.A)
			require.Equal(t, tc.expected.B, val.B)
			require.True(t, tc.expected.C.Equal(val.C))
			require.Equal(t, tc.expected.D != nil, val.D != nil)
			if tc.expected.D != nil {
				require.True(t, tc.expected.D.Equal(*val.D))
			}
			require.Equal(t, tc.expected.E, val.E)
			require.Equal(t, tc.expected.F, val.F)
		})
	}
}

func TestComplexStructQueryRowScan(t *testing.T) {
	type UnnamedEmbedding struct {
		UnnamedString string `db:"unnamed_string"`
	}
	type NamedEmbedding struct {
		NamedString string `db:"named_string"`
		UnnamedEmbedding
	}

	type PtrUnnamedEmbedding struct {
		UnnamedString string `db:"ptr_unnamed_string"`
	}
	type PtrNamedEmbedding struct {
		NamedString string `db:"ptr_named_string"`
		*PtrUnnamedEmbedding
	}

	type testStruct struct {
		A string `db:"a"`
		B NamedEmbedding
		C *PtrNamedEmbedding
		D struct {
			EmbeddedLiteralString string `db:"embedded_literal_string"`
		}
		E *struct {
			PtrEmbeddedLiteralString string `db:"ptr_embedded_literal_string"`
		}
	}

	tests := map[string]struct {
		query         string
		expected      testStruct
		expectedError error
	}{
		"All Properties": {
			query: `
			SELECT
				'a' as a,
				'named_string' as named_string,
				'unnamed_string' as unnamed_string,
				'ptr_named_string' as ptr_named_string,
				'ptr_unnamed_string' as ptr_unnamed_string,
				'embedded_literal_string' as embedded_literal_string,
				'ptr_embedded_literal_string' as ptr_embedded_literal_string
			`,
			expected: testStruct{
				A: "a",
				B: NamedEmbedding{
					NamedString: "named_string",
					UnnamedEmbedding: UnnamedEmbedding{
						UnnamedString: "unnamed_string",
					},
				},
				C: &PtrNamedEmbedding{
					NamedString: "ptr_named_string",
					PtrUnnamedEmbedding: &PtrUnnamedEmbedding{
						UnnamedString: "ptr_unnamed_string",
					},
				},
				D: struct {
					EmbeddedLiteralString string "db:\"embedded_literal_string\""
				}{
					EmbeddedLiteralString: "embedded_literal_string",
				},
				E: &struct {
					PtrEmbeddedLiteralString string "db:\"ptr_embedded_literal_string\""
				}{
					PtrEmbeddedLiteralString: "ptr_embedded_literal_string",
				},
			},
		},
		"Query returns only value structs not pointer structs": {
			query: `
			SELECT
				'a' as a,
				'named_string' as named_string,
				'unnamed_string' as unnamed_string,
				'embedded_literal_string' as embedded_literal_string
			`,
			expected: testStruct{
				A: "a",
				B: NamedEmbedding{
					NamedString: "named_string",
					UnnamedEmbedding: UnnamedEmbedding{
						UnnamedString: "unnamed_string",
					},
				},
				C: nil,
				D: struct {
					EmbeddedLiteralString string "db:\"embedded_literal_string\""
				}{
					EmbeddedLiteralString: "embedded_literal_string",
				},
				E: nil,
			},
			expectedError: ErrQueryColumnsTagsMismtach,
		},
	}
	ctx := context.Background()
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var val testStruct
			err := QueryRow(ctx, db, &val, tc.query)
			require.Equal(t, tc.expectedError, err)
			require.Equal(t, tc.expected, val)
		})
	}
}

func TestQueryRowScanDBTagBehaviour(t *testing.T) {
	type testStruct struct {
		A string `db:"a"`
		B string `db:"-"`

		C time.Time `db:"c"`
		D time.Time `db:"-"`

		E *time.Time `db:"e"`
		F *time.Time `db:"-"`
	}

	tests := map[string]struct {
		query         string
		expected      testStruct
		expectedError error
	}{
		"All Field with Tag value set": {
			query: `
			SELECT
			'a' as a,
			TIMESTAMPTZ '2006-01-02T15:04:05Z' as c,
			TIMESTAMPTZ '2006-01-02T15:04:05Z' as e
			`,
			expected: testStruct{
				A: "a",
				C: *pointInTimePtr(t),
				E: pointInTimePtr(t),
			},
			expectedError: nil,
		},
		"Fail to set ignored properties": {
			query: `
			SELECT
			'a' as a,
			'b' as b,
			TIMESTAMPTZ '2006-01-02T15:04:05Z' as c,
			TIMESTAMPTZ '2006-01-02T15:04:05Z' as d,
			TIMESTAMPTZ '2006-01-02T15:04:05Z' as e,
			TIMESTAMPTZ '2006-01-02T15:04:05Z' as f
			`,
			expected: testStruct{
				A: "a",
				C: *pointInTimePtr(t),
				E: pointInTimePtr(t),
			},
			expectedError: &ErrQueryReturnedExtraColumns{
				ValueType: "*pgxscan.testStruct",
				Columns: []string{
					"b",
					"d",
					"f",
				},
			},
		},
	}

	ctx := context.Background()
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var val testStruct
			err := QueryRow(ctx, db, &val, tc.query)
			require.Equal(t, tc.expectedError, err)

			require.Equal(t, tc.expected.A, val.A)
			require.Equal(t, tc.expected.B, val.B)

			require.True(t, tc.expected.C.Equal(val.C))
			require.True(t, tc.expected.D.Equal(val.D))

			require.Equal(t, tc.expected.E != nil, val.E != nil)
			if tc.expected.E != nil {
				require.True(t, tc.expected.E.Equal(*val.E))
			}

			require.Equal(t, tc.expected.F != nil, val.F != nil)
			if tc.expected.F != nil {
				require.True(t, tc.expected.F.Equal(*val.F))
			}
		})
	}
}

func TestPrivateProperty(t *testing.T) {
	type testStruct struct {
		a string `db:"a"`
	}

	var val testStruct
	ctx := context.Background()
	err := QueryRow(ctx, db, &val,
		`
	SELECT
		'something' as a
	`)

	require.Equal(t, ErrUnexportedProperty{
		PropertyName: "a",
	}, err)
}

func TestPrivateEmbededType(t *testing.T) {
	type a struct {
		B string `db:"b"`
	}

	type testStruct struct {
		private a
	}

	var val testStruct
	ctx := context.Background()
	err := QueryRow(ctx, db, &val,
		`
	SELECT
		'something' as b
	`)
	fmt.Println(err)

	//todo: Find a way to detect this error path, and report a valid error to the user
}

func TestEmbededStructWithPrivateProperty(t *testing.T) {
	type A struct {
		b string `db:"b"`
	}

	type testStruct struct {
		A
	}

	var val testStruct
	ctx := context.Background()
	err := QueryRow(ctx, db, &val,
		`
	SELECT
		'something' as b
	`)

	require.Equal(t, ErrUnexportedProperty{
		PropertyName: "b",
	}, err)
}
