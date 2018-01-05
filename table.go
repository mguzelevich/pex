package pex

import (
	"bytes"
	"fmt"
	//"os"
	"regexp"
	"strings"
)

var skippable map[string]bool

type column struct {
	path     path
	Type     string
	Nullable bool
	Pk       bool
	fks      map[path]bool
}

func (c *column) UID() string {
	return c.path.UID()
}

func (c *column) Name() string {
	return c.path.column
}

func (c *column) Fks() []path {
	fks := []path{}
	for ref := range c.fks {
		fks = append(fks, ref)
	}
	return fks
}

func (c *column) getMarkdown(buffer *bytes.Buffer) {
	// fmt.Printf("%3v | %8v | %6v | %6v\n", uid, username, department, created)

	if c.Pk {
		fmt.Fprintf(buffer, "[PK] ")
	} else {
		fmt.Fprintf(buffer, "     ")
	}
	fmt.Fprintf(buffer, "%s %s", c.Name(), c.Type)
	if c.Nullable {
		fmt.Fprintf(buffer, " NULL")
	}
	if len(c.fks) > 0 {
		fmt.Fprintf(buffer, "\t->")
		for ref := range c.fks {
			fmt.Fprintf(buffer, " %s", ref.uid())
		}
	}
	fmt.Fprintf(buffer, "\n")
}

type table struct {
	path          path
	columns       map[string]*column
	columns_order []string
}

func (t *table) UID() string {
	uid := strings.Replace(t.path.uid(), ".", "_", -1)
	return uid
}

func (t *table) Name() string {
	return t.path.table
}

func (t *table) Columns() []*column {
	columns := []*column{}
	for _, c := range t.columns_order {
		columns = append(columns, t.columns[c])
	}
	return columns
}

func (t *table) ColumnNames() string {
	names := []string{}
	for _, c := range t.columns_order {
		col := t.columns[c]
		buff := &bytes.Buffer{}
		fmt.Fprintf(buff, "<%s>%s", col.UID(), col.Name())
		if col.Pk {
			fmt.Fprintf(buff, "[PK]")
		}
		if len(col.fks) > 0 {
			fmt.Fprintf(buff, "[FK]")
		}
		names = append(names, buff.String())
	}
	return strings.Join(names, "|")
}

func (t *table) ColumnTypes() string {
	types := []string{}
	for _, c := range t.columns_order {
		col := t.columns[c]
		buff := &bytes.Buffer{}
		fmt.Fprintf(buff, "%s", col.Type)
		if col.Nullable {
			fmt.Fprintf(buff, " [NULL]")
		}
		types = append(types, buff.String())
	}
	return strings.Join(types, "|")
}

func (t *table) column(p path) *column {
	return t.columns[p.column]
}

func (t *table) AddColumn(colPath path, dataType string, nullable bool) bool {
	name := colPath.column
	col := &column{
		path:     colPath,
		fks:      make(map[path]bool),
		Type:     dataType,
		Nullable: nullable,
	}
	t.columns[name] = col
	t.columns_order = append(t.columns_order, name)
	return true
}

func (t *table) AddFk(fk path, ref path) {
	c := t.column(fk)
	if _, ok := c.fks[ref]; ok {
		//panic("fk raise")
	}
	c.fks[ref] = true
}

func (t *table) renderable() bool {
	for r := range skippable {
		if regexp.MustCompile(r).MatchString(t.Name()) {
			return false
		}
	}
	return true
}

func NewTable(p path) *table {
	return &table{
		path:    p,
		columns: make(map[string]*column),
	}
}

type tables []*table

func (t tables) Len() int {
	return len(t)
}

func (t tables) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t tables) Less(i, j int) bool {
	return t[i].Name() < t[j].Name()
}

func init() {
	skippable = map[string]bool{
		"goose_db_version": true,
		"_part_[0-9]+":     true,
		"spatial_ref_sys":  true,
	}
}
