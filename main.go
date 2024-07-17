package struct2sql

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"unicode"
)

type CreateTableOpts struct {
	TablePrefix    string
	PrependColumns string
	IncludeFields  map[string]bool
	ExcludeFields  map[string]bool
}

type InsertOpts struct {
	TablePrefix string
	PrependColumns string
	PrependValues string
	IncludeFields  map[string]bool
	ExcludeFields  map[string]bool
}

func CreateTable(u interface{}, opts *CreateTableOpts) string {
	v := reflect.ValueOf(u)
	i := reflect.Indirect(v)
	s := i.Type()
	usName := getUnderscoredName(s.Name())

	dbTable := getPluralName(usName)
	if opts != nil && opts.TablePrefix != "" {
		dbTable = opts.TablePrefix + dbTable
	}

	includeFields := map[string]bool{}
	if opts != nil {
		includeFields = opts.IncludeFields
	}
	excludeFields := map[string]bool{}
	if opts != nil {
		excludeFields = opts.ExcludeFields
	}

	cols := getColumnListFromType(s, "", "", false, includeFields, excludeFields, true, false)
	if opts != nil && opts.PrependColumns != "" {
		newCols := strings.TrimSpace(opts.PrependColumns)
		if !strings.HasSuffix(newCols, ",") {
			newCols += ","
		}
		newCols += cols
		cols = newCols
	}

	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", dbTable, cols)
}

func Insert(u interface{}, opts *InsertOpts) string {
	v := reflect.ValueOf(u)
	i := reflect.Indirect(v)
	s := i.Type()
	usName := getUnderscoredName(s.Name())

	dbTable := getPluralName(usName)
	if opts != nil && opts.TablePrefix != "" {
		dbTable = opts.TablePrefix + dbTable
	}

	includeFields := map[string]bool{}
	if opts != nil {
		includeFields = opts.IncludeFields
	}
	excludeFields := map[string]bool{}
	if opts != nil {
		excludeFields = opts.ExcludeFields
	}

	cols := getColumnListFromType(s, "", "", false, includeFields, excludeFields, true, true)
	colsLen := len(strings.Split(cols, ","))
	vals := strings.TrimRight(strings.Repeat("?,",colsLen), ",")
	if opts != nil && opts.PrependColumns != "" {
		newCols := strings.TrimSpace(opts.PrependColumns)
		if !strings.HasSuffix(newCols, ",") {
			newCols += ","
		}
		newCols += cols
		cols = newCols
	}
	if opts != nil && opts.PrependValues != "" {
		newVals := strings.TrimSpace(opts.PrependValues)
		if !strings.HasSuffix(newVals, ",") {
			newVals += ","
		}
		newVals += vals
		vals = newVals
	}
	

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", dbTable, cols, vals)
}

func getColumnListFromType(t reflect.Type, colPrefix string, parentFieldName string, parentFieldPointer bool, includeFields map[string]bool, excludeFields map[string]bool, goDeeper bool, onlyCols bool) string {
	cols := ""
	for j := 0; j < t.NumField(); j++ {
		field := t.Field(j)

		if parentFieldName == "" && len(includeFields) > 0 && !includeFields[field.Name] {
			continue
		}
		if parentFieldName == "" && len(excludeFields) > 0 && excludeFields[field.Name] {
			continue
		}
		if parentFieldName != "" && len(includeFields) > 0 && !includeFields[parentFieldName+"."+field.Name] && !includeFields[parentFieldName+"."] {
			continue
		}
		if parentFieldName != "" && len(excludeFields) > 0 && excludeFields[parentFieldName+"."+field.Name] {
			continue
		}

		colName := colPrefix + getColumnNameFromField(field)
		colType := getColumnTypeFromField(field, parentFieldPointer)

		if colName != "" && colType != "" {
			if cols != "" {
				cols += ","
			}
			cols += colName 
			if !onlyCols {
				cols += " " + colType
			}
		}

		structName := getStructName(field)
		if structName != "" && goDeeper {
			colPrefix := getColumnNameFromField(field) + "_"
			colsInside := ""
			if isFieldTypePointer(field) {
				colsInside = getColumnListFromType(field.Type.Elem(), colPrefix, field.Name, isFieldTypePointer(field), includeFields, excludeFields, false, onlyCols)
			} else {
				colsInside = getColumnListFromType(field.Type, colPrefix, field.Name, isFieldTypePointer(field), includeFields, excludeFields, false, onlyCols)
			}
			if colsInside != "" {
				if cols != "" {
					cols += ","
				}
				cols += colsInside
			}
		}
	}
	return cols
}

func isFieldTypePointer(field reflect.StructField) bool {
	return field.Type.Kind() == reflect.Pointer
}

func getStructName(field reflect.StructField) string {
	var kind reflect.Kind
	var typeName string
	if isFieldTypePointer(field) {
		kind = field.Type.Elem().Kind()
		typeName = field.Type.Elem().Name()
	} else {
		kind = field.Type.Kind()
		typeName = field.Type.Name()
	}

	if kind == reflect.Struct {
		return typeName
	}

	return ""
}

func getColumnNameFromField(field reflect.StructField) string {
	return getUnderscoredName(field.Name)
}

func getColumnTypeFromField(field reflect.StructField, forceNull bool) string {
	typeIsPointer := isFieldTypePointer(field)

	var kind reflect.Kind
	var typeName string
	if typeIsPointer {
		kind = field.Type.Elem().Kind()
		typeName = field.Type.Elem().Name()
	} else {
		kind = field.Type.Kind()
		typeName = field.Type.Name()
	}

	colType := ""
	if kind == reflect.String {
		colType = "TEXT"
	} else if kind == reflect.Int || kind == reflect.Int64 {
		colType = "INT"
	} else if kind == reflect.Bool {
		colType = "BOOLEAN"
	} else if kind == reflect.Struct && typeName == "Timestamp" {
		colType = "DATETIME"
	}

	if forceNull || (colType != "" && typeIsPointer) {
		colType += " NULL"
	}

	return colType
}

func getUnderscoredName(s string) string {
	// Match all acronyms and leave only first letter capital, eg. 'SQL' becomes 'Sql'
	re := regexp.MustCompile(`[A-Z][A-Z0-9]+`)
	for _, found := range re.FindAllString(s, -1) {
		n := fmt.Sprintf("%s%s", string(found[0]), strings.ToLower(found)[1:])
		s = strings.Replace(s, found, n, -1)
	}

	o := ""
	var prev rune
	for i, ch := range s {
		if i == 0 {
			o += strings.ToLower(string(ch))
		} else {
			if unicode.IsUpper(ch) {
				if prev == 'I' && ch == 'D' {
					o += strings.ToLower(string(ch))
				} else {
					o += "_" + strings.ToLower(string(ch))
				}
			} else {
				o += string(ch)
			}
		}
		prev = ch
	}
	return o
}

func getPluralName(s string) string {
	re := regexp.MustCompile(`y$`)
	if re.MatchString(s) {
		return string(re.ReplaceAll([]byte(s), []byte(`ies`)))
	}
	re = regexp.MustCompile(`s$`)
	if re.MatchString(s) {
		return s + "es"
	}
	return s + "s"
}
