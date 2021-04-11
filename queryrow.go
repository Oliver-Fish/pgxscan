package pgxscan

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"unicode"

	"github.com/jackc/pgx/v4"
)

//QueryRow is a wrapper around Query that allows us to avoid the verbose Scan call
func QueryRow(ctx context.Context, tx querier, input interface{}, query string, args ...interface{}) error {
	rv := reflect.ValueOf(input)
	if !rv.IsValid() {
		return fmt.Errorf("input value in invalid")
	}

	rt := rv.Type()
	if rt.Kind() != reflect.Ptr {
		return fmt.Errorf("input value is not a pointer")
	}

	rt = rt.Elem()
	if rt.Kind() != reflect.Struct {
		return fmt.Errorf("input value is not a pointer to a struct")
	}

	dbTagPos, err := getDBTagPositions(rt)
	if err != nil {
		return err
	}

	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		err = rows.Err()
		if err != nil {
			return err
		}

		return pgx.ErrNoRows
	}

	headers := rows.FieldDescriptions()

	structVal := rv.Elem()

	var extracolumnsError *ErrQueryReturnedExtraColumns
	var rejectedValues interface{}
	fieldPtrs := make([]interface{}, len(headers))
	for ii, header := range headers {
		fieldPos, ok := dbTagPos[string(header.Name)]
		if !ok {
			//If the query returns a column the struct doesn't have this is a wasteful action so we
			//build an error that will be returned, however we continue the operation as we don't
			//need to fail and it's up to the caller to decide if this is ok
			if extracolumnsError == nil {
				extracolumnsError = &ErrQueryReturnedExtraColumns{
					ValueType: fmt.Sprintf("%T", input),
					Columns:   nil,
				}
			}

			extracolumnsError.Columns = append(extracolumnsError.Columns, string(header.Name))

			fieldPtrs[ii] = &rejectedValues
			continue

		}

		//Check if we are doing a nested lookup
		if len(fieldPos) > 1 {
			currentStruct := structVal
			//Check if path to field we want is safe, i.e no nil pointers
			for i := 0; i <= len(fieldPos)-2; i++ {
				if currentStruct.Kind() == reflect.Ptr {
					currentStruct = currentStruct.Elem()
				}
				pos := fieldPos[i]
				innerValue := currentStruct.FieldByIndex([]int{pos})
				//If we aren't dealing with pointers then we are safe so do nothing
				if innerValue.Kind() != reflect.Ptr {
					continue
				}
				if !innerValue.CanAddr() {
					return errors.New("unable to get address of field ")
				}

				if innerValue.IsNil() {
					innerValue.Set(reflect.New(innerValue.Type().Elem()))
				}

				//safe path
				currentStruct = innerValue
				continue

			}
		}

		//Below we get a pointer to each field matching a header returned from the query
		//This allows us to directly update the field in requires structs without touching data we shouldn't
		fieldVal := structVal.FieldByIndex(fieldPos)

		if !fieldVal.CanAddr() {
			return errors.New("unable to get address of field")
		}

		fieldPtr := fieldVal.Addr()
		if !fieldPtr.CanInterface() {
			propertyName := structVal.Type().FieldByIndex(fieldPos).Name
			//If the property is unexported we can return a more detailed error
			if unicode.IsLower(rune(propertyName[0])) {
				return ErrUnexportedProperty{
					PropertyName: propertyName,
				}
			}
			return errors.New("unable to convert pointer of field to interface")
		}
		fieldPtrs[ii] = fieldPtr.Interface()
	}

	headers = rows.FieldDescriptions()
	err = rows.Scan(fieldPtrs...)
	if err != nil {
		return err
	}

	if rows.Next() {
		return errors.New("query returned more than one row")
	}

	if extracolumnsError != nil {
		return extracolumnsError
	}

	columnCount := len(headers)
	if len(dbTagPos) != columnCount {
		return ErrQueryColumnsTagsMismtach
	}

	return nil
}
