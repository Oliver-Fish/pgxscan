package pgxscan

import (
	"fmt"
	"strings"
)

// ErrQueryColumnsTagsMismtach is returned when not all struct tags count does not match query column count
// This is a non-fatal error and can be ignored if the above is by design or planned
// This however acts as a fail-safe to avoid missing columns inside your select calls
var ErrQueryColumnsTagsMismtach = fmt.Errorf("query returned less columns than DB tags on struct")

type ErrUnexportedProperty struct {
	PropertyName string
}

func (err ErrUnexportedProperty) Error() string {
	return fmt.Sprintf("unable to access unexported field '%s'", err.PropertyName)
}

type ErrQueryReturnedExtraColumns struct {
	ValueType string
	Columns   []string
}

func (err ErrQueryReturnedExtraColumns) Error() string {
	columnOptionalPlural := "column"
	tagOptionalPlural := "tag"
	if len(err.Columns) > 1 {
		columnOptionalPlural = "columns"
		tagOptionalPlural = "tags"
	}

	return fmt.Sprintf("query returned %s %s the supplied struct of type %T does not contain these %s",
		columnOptionalPlural,
		strings.Join(err.Columns, ","),
		nil,
		tagOptionalPlural,
	)
}
