package pgxscan

import (
	"context"
	"fmt"
	"reflect"

	"github.com/jackc/pgx/v4"
)

type querier interface {
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
}

func getDBTagPositions(rt reflect.Type) (map[string][]int, error) {
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
