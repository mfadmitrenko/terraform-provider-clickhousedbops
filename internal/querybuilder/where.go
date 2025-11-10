package querybuilder

import (
	"fmt"
	"reflect"
)

type Where interface {
	Clause() string
}

type simpleWhere struct {
	field    string
	value    interface{}
	operator string
}

func WhereEquals(fieldName string, value interface{}) Where {
	return &simpleWhere{
		field:    fieldName,
		value:    value,
		operator: "=",
	}
}

func WhereDiffers(fieldName string, value interface{}) Where {
	return &simpleWhere{
		field:    fieldName,
		value:    value,
		operator: "<>",
	}
}

func IsNull(fieldName string) Where {
	return &simpleWhere{
		field: fieldName,
		value: nil,
	}
}

func (s *simpleWhere) Clause() string {
	if isNilValue(s.value) {
		predicate := "IS NULL"
		if s.operator == "<>" {
			predicate = "IS NOT NULL"
		}

		return fmt.Sprintf("%s %s", backtick(s.field), predicate)
	}

	if str, ok := stringValue(s.value); ok {
		return fmt.Sprintf("%s %s %s", backtick(s.field), s.operator, quote(str))
	}

	return fmt.Sprintf("%s %s %v", backtick(s.field), s.operator, s.value)
}

func isNilValue(value interface{}) bool {
	if value == nil {
		return true
	}

	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

func stringValue(value interface{}) (string, bool) {
	switch v := value.(type) {
	case string:
		return v, true
	case fmt.Stringer:
		return v.String(), true
	case *string:
		if v == nil {
			return "", false
		}
		return *v, true
	}

	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Pointer:
		if rv.IsNil() {
			return "", false
		}
		return stringValue(rv.Elem().Interface())
	case reflect.String:
		return rv.String(), true
	}

	return "", false
}
