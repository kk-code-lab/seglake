package main

import (
	"encoding/json"
	"fmt"
	"reflect"
)

func writeJSON(v any) error {
	data, err := json.MarshalIndent(normalizeJSONValue(v), "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

func normalizeJSONValue(v any) any {
	if v == nil {
		return nil
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice:
		if rv.IsNil() {
			return reflect.MakeSlice(rv.Type(), 0, 0).Interface()
		}
	case reflect.Map:
		if rv.IsNil() {
			return reflect.MakeMap(rv.Type()).Interface()
		}
	}
	return v
}
