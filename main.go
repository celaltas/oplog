package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

)

const (
	Invalid = iota
	Object
	Array
)

type OplogEntry struct {
	Operation string                 `json:"op"`
	NameSpace string                 `json:"ns"`
	Data      map[string]interface{} `json:"o"`
	Where     map[string]interface{} `json:"o2"`
}

func main() {
	fmt.Println("Hello, World!")

}

func GenerateSQL(oplog string) ([]string, error) {

	var oplogEntries []OplogEntry
	var queries []string

	jsonType := getJSONType(oplog)
	if jsonType == Object {
		var oplogObj OplogEntry
		if err := json.Unmarshal([]byte(oplog), &oplogObj); err != nil {
			return nil, err
		}
		oplogEntries = append(oplogEntries, oplogObj)
	} else if jsonType == Array {
		if err := json.Unmarshal([]byte(oplog), &oplogEntries); err != nil {
			return nil, err
		}

	} else {
		return nil, fmt.Errorf("invalid json type")
	}

	cache := make(map[string]bool)

	for _, opl := range oplogEntries {
		query, err := GenerateSQLFromSingleOPlog(opl, cache)
		if err != nil {
			return nil, err
		}
		queries = append(queries, query...)
	}

	return queries, nil

}

func GenerateSQLFromSingleOPlog(oplog OplogEntry, cache map[string]bool) ([]string, error) {
	var queries []string
	switch oplog.Operation {
	case "i":

		schema := strings.Split(oplog.NameSpace, ".")[0]
		exist := cache[schema]
		if !exist {
			queries = append(queries, fmt.Sprintf("CREATE SCHEMA %s;", schema))
			cache[schema] = true
		}
		exist = cache[oplog.NameSpace]
		if !exist {
			tableQuery, err := GenerateTable(oplog, cache)
			if err != nil {
				return queries, err
			}
			queries = append(queries, tableQuery)
			cache[oplog.NameSpace] = true
		} else {
			unalteredColumns, err := GetUnalteredColumns(oplog, cache)
			if err != nil {
				return queries, err
			}
			if len(unalteredColumns) > 0 {
				alterQuery, err := GenerateAlterTable(oplog, unalteredColumns)
				if err != nil {
					return queries, err
				}
				queries = append(queries, alterQuery)
			}
		}
		query, err := GenerateInsertSQL(oplog)
		if err != nil {
			return queries, err
		}
		queries = append(queries, query)
	case "u":
		query, err := GenerateUpdateSQL(oplog)
		if err != nil {
			return queries, err
		}
		queries = append(queries, query)
	case "d":
		query, err := GenerateDeleteSQL(oplog)
		if err != nil {
			return queries, err
		}
		queries = append(queries, query)
	default:
		return queries, fmt.Errorf("invalid operation log")
	}
	return queries, nil
}

func GenerateAlterTable(oplog OplogEntry, unalteredColumns []string) (string, error) {
	sql := fmt.Sprintf("ALTER TABLE %s ADD %s;", oplog.NameSpace, strings.Join(unalteredColumns, ", "))
	return sql, nil
}

func GetUnalteredColumns(oplog OplogEntry, cache map[string]bool) ([]string, error) {
	var columns []string
	for column, value := range oplog.Data {
		key := fmt.Sprintf("%s.%s", oplog.NameSpace, column)
		if !cache[key] {
			sqlType, err := ConvertSQLType(reflect.TypeOf(value))
			if err != nil {
				return nil, err
			}
			exp := fmt.Sprintf("%s %s %s", column, sqlType, strings.Join(GetColumnConstraints(column), " "))
			columns = append(columns, exp)
		}
	}
	return columns, nil
}

func GenerateTable(oplog OplogEntry, cache map[string]bool) (string, error) {
	columns := make([]string, 0)
	var columnNames []string
	for column := range oplog.Data {
		columnNames = append(columnNames, column)
	}
	sort.Strings(columnNames)
	for _, col := range columnNames {
		sqlType, err := ConvertSQLType(reflect.TypeOf(oplog.Data[col]))
		if err != nil {
			return "", err
		}
		key := fmt.Sprintf("%s.%s", oplog.NameSpace, col)
		cache[key] = true
		exp := fmt.Sprintf("%s %s%s", col, sqlType, strings.Join(GetColumnConstraints(col), " "))
		columns = append(columns, exp)
	}
	sql := fmt.Sprintf("CREATE TABLE %s (%s);", oplog.NameSpace, strings.Join(columns, ", "))
	return sql, nil
}

func GenerateInsertSQL(oplog OplogEntry) (string, error) {
	columns := make([]string, 0)
	values := make([]string, 0)
	var columnNames []string

	for column := range oplog.Data {
		columnNames = append(columnNames, column)
	}

	sort.Strings(columnNames)

	for _, col := range columnNames {
		value := oplog.Data[col]
		columns = append(columns, col)
		values = append(values, ConvertToString(value))
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", oplog.NameSpace, strings.Join(columns, ", "), strings.Join(values, ", "))
	return sql, nil
}

func GenerateUpdateSQL(oplog OplogEntry) (string, error) {
	diffMap, ok := oplog.Data["diff"].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("invalid operation log")
	}
	setMap, setMapOK := diffMap["u"].(map[string]interface{})
	unsetMap, unsetMapOK := diffMap["d"].(map[string]interface{})
	var updateValues []string
	if setMapOK {
		setKeys := make([]string, 0)
		for column := range setMap {
			setKeys = append(setKeys, column)
		}
		sort.Strings(setKeys)
		for _, column := range setKeys {
			value := setMap[column]
			updateValues = append(updateValues, fmt.Sprintf("%s = %s", column, ConvertToString(value)))
		}
	}
	if unsetMapOK {
		unsetKeys := make([]string, 0)
		for column := range unsetMap {
			unsetKeys = append(unsetKeys, column)
		}
		sort.Strings(unsetKeys)
		for _, column := range unsetKeys {
			updateValues = append(updateValues, fmt.Sprintf("%s = NULL", column))
		}
	}

	whereClauseCols := make([]string, 0)
	for column, value := range oplog.Where {
		whereClauseCols = append(whereClauseCols, fmt.Sprintf("%s = %s", column, ConvertToString(value)))
	}
	sql := fmt.Sprintf("UPDATE %s SET %s WHERE %s;", oplog.NameSpace, strings.Join(updateValues, ", "), strings.Join(whereClauseCols, " AND "))
	return sql, nil
}

func GenerateDeleteSQL(oplog OplogEntry) (string, error) {
	whereClauseCols := make([]string, 0)
	var columnNames []string
	for column := range oplog.Data {
		columnNames = append(columnNames, column)
	}
	sort.Strings(columnNames)
	for _, column := range columnNames {
		value := oplog.Data[column]
		whereClauseCols = append(whereClauseCols, fmt.Sprintf("%s = %s", column, ConvertToString(value)))
	}
	sql := fmt.Sprintf("DELETE FROM %s WHERE %s;", oplog.NameSpace, strings.Join(whereClauseCols, " AND "))
	return sql, nil
}

func ConvertToString(value interface{}) string {
	switch v := value.(type) {
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return fmt.Sprintf("%v", v)
	case string:
		return fmt.Sprintf("'%v'", v)
	case bool:
		return fmt.Sprintf("%t", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func ConvertSQLType(v reflect.Type) (string, error) {
	switch v.Kind() {
	case reflect.String:
		return "VARCHAR(255)", nil
	case reflect.Bool:
		return "BOOLEAN", nil
	case reflect.Float32, reflect.Float64:
		return "FLOAT", nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return "SMALLINT", nil
	case reflect.Int64:
		return "BIGINT", nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return "SMALLINT", nil
	case reflect.Uint64:
		return "BIGINT", nil
	case reflect.Struct:
		if v == reflect.TypeOf(time.Time{}) {
			return "TIMESTAMP", nil
		}
	}
	return "", fmt.Errorf("unsupported Go type: %s", v.String())
}

func GetColumnConstraints(col string) []string {
	var constraints []string
	if col == "_id" {
		constraints = append(constraints, " PRIMARY KEY")
	}
	return constraints
}

func getJSONType(jsonStr string) int {
	if len(jsonStr) == 0 {
		return Invalid
	}
	if jsonStr[0] == '{' && jsonStr[len(jsonStr)-1] == '}' {
		return Object
	}
	if jsonStr[0] == '[' && jsonStr[len(jsonStr)-1] == ']' {
		return Array
	}
	return 0
}
