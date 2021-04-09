package pgxscan

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/jackc/pgx/v4"
)

// ErrQueryColumnsTagsMismtach is returned when not all struct tags count does not match query column count
// This is a non-fatal error and can be ignored if the above is by design or planned
// This however acts as a fail-safe to avoid missing columns inside your select calls
var ErrQueryColumnsTagsMismtach = fmt.Errorf("query returned less columns than DB tags on struct")

type querier interface {
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
}

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

	fieldPtrs := make([]interface{}, len(headers))
	for ii, header := range headers {
		fieldPos, ok := dbTagPos[string(header.Name)]
		if !ok {
			//If the query returns a column the struct doesn't have this is a wasteful action so we fail
			return fmt.Errorf("query returned column %s that is missing from passed struct", string(header.Name))
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

	columnCount := len(headers)
	if len(dbTagPos) != columnCount {
		return ErrQueryColumnsTagsMismtach
	}

	return nil
}

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

func getDBTagPositions(rt reflect.Type) (map[string][]int, error) {
	/*
		TODO:
			Write tests
			Validate what happens with an embeded pointer or a pointer to a struct that requires deeper lookup
	*/
	if rt.Kind() != reflect.Struct {
		return nil, fmt.Errorf("reflect type is not a struct")
	}

	tagPositions := make(map[string][]int)

	for i := 0; i < rt.NumField(); i++ {
		field := rt.Field(i)

		switch field.Type.Kind() {
		case reflect.Struct:
			tag := field.Tag.Get("db")
			if tag == "-" {
				//If an embeded struct has a ignore db tag
				//skip entire struct lookup, in this case we shouldn't have a tag
				continue
			}
			if tag != "" {
				//Tag Found so add it to the list and don't go deeper
				tagPositions[tag] = field.Index
				continue
			}

			//Get all tags on nested struct
			nestedTags, err := getDBTagPositions(field.Type)
			if err != nil {
				return nil, err
			}

			//Add all nested positions to top level map
			for t, ni := range nestedTags {
				tagPositions[t] = append([]int{i}, ni...)
			}

		case reflect.Ptr:
			underlineType := field.Type.Elem()
			if underlineType.Kind() == reflect.Struct {
				//Get all tags on nested struct
				nestedTags, err := getDBTagPositions(underlineType)
				if err != nil {
					return nil, err
				}

				//Add all nested positions to top level map
				for t, ni := range nestedTags {
					tagPositions[t] = append([]int{i}, ni...)
				}
				continue
			}
			//If we have a pointer that doesn't point to a struct then we don't need to look deeper
			fallthrough
		default:
			tag := field.Tag.Get("db")
			//If we find a case where no tag is set return error
			//tags should either be set or have a dash to be ignored
			if tag == "" {
				return nil, fmt.Errorf("unset tag on property %d of struct %s", i, rt.String())
			}

			if tag == "-" {
				continue
			}

			tagPositions[tag] = field.Index

		}
	}

	return tagPositions, nil
}
