package pgxscan

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/jackc/pgx/v4"
)

//Rows takes a pgx.Rows and pointer to a slice of struct
//It will simplify scanning by using the db tags on structs to avoid verbose Scan calls
func Rows(rows pgx.Rows, input interface{}) error {
	defer rows.Close()

	//Input Validation logic
	rv := reflect.ValueOf(input)
	if !rv.IsValid() {
		return fmt.Errorf("input value in invalid")
	}

	rt := rv.Type()
	if rt.Kind() != reflect.Ptr {
		return fmt.Errorf("input value is not a pointer")
	}

	rt = rt.Elem()
	if rt.Kind() != reflect.Slice {
		return fmt.Errorf("input value is not a pointer to a slice")
	}

	rt = rt.Elem()
	if rt.Kind() != reflect.Struct {
		return fmt.Errorf("input value is not a pointer to a slice of struct")
	}

	dbTagPos, err := getDBTagPositions(rt)
	if err != nil {
		return err
	}

	if rv.Elem().Len() > 0 {
		//We are working with a slide that already has data
		//We have to work with the existing values and destroy the original dataset
		//If the query returns more rows than our slice already has we will error
		err = scanToExistingSlice(rows, rt, rv, dbTagPos)
		if err != nil {
			return err
		}
	} else {

		//Slice is empty so we can freely add values to the slice
		return scanToNewSlice(rows, rt, rv, dbTagPos)
	}

	columnCount := len(rows.FieldDescriptions())
	if len(dbTagPos) != columnCount {
		return ErrQueryColumnsTagsMismtach
	}

	return nil

}

func scanToExistingSlice(rows pgx.Rows, rt reflect.Type, rv reflect.Value, dbTagPos map[string][]int) error {
	slice := rv.Elem()
	sliceLen := slice.Len()

	if !rows.Next() {
		err := rows.Err()
		if err != nil {
			return err
		}
		return nil
	}

	headers := rows.FieldDescriptions()
	fieldPtrs := make([]interface{}, len(headers))
	for i := 0; ; i++ {
		if i > sliceLen-1 {
			return errors.New("query returned more rows that slice length")
		}

		structVal := slice.Index(i)

		for ii, header := range headers {
			fieldPos, ok := dbTagPos[string(header.Name)]
			if !ok {
				//If the query returns a column the struct doesn't have this is a wasteful action so we fail
				return fmt.Errorf("query returned column %s that is missing from passed struct", string(header.Name))
			}

			//Below we get a pointer to each field matching a header returned from the query
			//This allows us to directly update the field in requires structs without touching data we shouldn't
			fieldVal := structVal.FieldByIndex(fieldPos)

			if !fieldVal.CanAddr() {
				return errors.New("unable to get address of field")
			}

			fieldPtr := fieldVal.Addr()
			if !fieldPtr.CanInterface() {
				return errors.New("unable to convert pointer of field to interface")
			}
			fieldPtrs[ii] = fieldPtr.Interface()
		}
		err := rows.Scan(fieldPtrs...)
		if err != nil {
			return err
		}

		if !rows.Next() {
			break
		}
	}

	err := rows.Err()
	if err != nil {
		return err
	}

	columnCount := len(headers)
	if len(dbTagPos) != columnCount {
		return ErrQueryColumnsTagsMismtach
	}

	return nil
}

func scanToNewSlice(rows pgx.Rows, rt reflect.Type, rv reflect.Value, dbTagPos map[string][]int) error {
	if !rows.Next() {
		err := rows.Err()
		if err != nil {
			return err
		}
		return nil
	}

	headers := rows.FieldDescriptions()

	outputSlice := reflect.MakeSlice(rv.Elem().Type(), 0, 1)

	fieldPtrs := make([]interface{}, len(headers))

	for i := 0; ; i++ {
		outputStruct := reflect.New(rt)
		for ii, header := range headers {
			fieldPos, ok := dbTagPos[string(header.Name)]
			if !ok {
				//If the query returns a column the struct doesn't have this is a wasteful action so we fail
				return fmt.Errorf("query returned column %s that is missing from passed struct", string(header.Name))
			}
			//Below we get a pointer to each field matching a header returned from the query
			//This allows us to directly update the field in requires structs without touching data we shouldn't
			fieldVal := outputStruct.Elem().FieldByIndex(fieldPos)

			if !fieldVal.CanAddr() {
				return errors.New("unable to get address of field")
			}

			fieldPtr := fieldVal.Addr()
			if !fieldPtr.CanInterface() {
				return errors.New("unable to convert pointer of field to interface")
			}
			fieldPtrs[ii] = fieldPtr.Interface()
		}
		err := rows.Scan(fieldPtrs...)
		if err != nil {
			return err
		}
		outputSlice = reflect.Append(outputSlice, outputStruct.Elem())
		// si := outputSlice.Index(i)
		// if !si.CanSet() {
		// 	return errors.New("unable to set slice on output slice")
		// }
		// si.Set(outputStruct.Elem())

		if !rows.Next() {
			break
		}
	}

	rv.Elem().Set(outputSlice)

	err := rows.Err()
	if err != nil {
		return err
	}

	columnCount := len(headers)
	if len(dbTagPos) != columnCount {
		return ErrQueryColumnsTagsMismtach
	}

	return nil
}
