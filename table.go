package pex

import (
	"bytes"
	"fmt"
	"os"
)

var skippable map[string]bool

type column struct {
	name     string
	dataType string
	nullable bool
	pk       bool
	fks      map[path]bool
}

func (c *column) out() {
	output := bytes.Buffer{}

	// fmt.Printf("%3v | %8v | %6v | %6v\n", uid, username, department, created)

	if c.pk {
		fmt.Fprintf(&output, "[PK] ")
	} else {
		fmt.Fprintf(&output, "     ")
	}
	fmt.Fprintf(&output, "%s %s", c.name, c.dataType)
	if c.nullable {
		fmt.Fprintf(&output, " NULL")
	}
	if len(c.fks) > 0 {
		fmt.Fprintf(&output, "\t->")
		for ref := range c.fks {
			fmt.Fprintf(&output, " %s", ref.uid())
		}
	}
	fmt.Fprintf(os.Stderr, "%s\n", output.String())
}

type table struct {
	path          path
	columns       map[string]*column
	columns_order []string
}

func (t *table) column(p path) *column {
	return t.columns[p.column]
}

func (t *table) AddColumn(colPath path, dataType string, nullable bool) bool {
	name := colPath.column
	col := &column{
		name:     name,
		dataType: dataType,
		nullable: nullable,
		fks:      make(map[path]bool),
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

func (t *table) uid() string {
	return fmt.Sprintf("%s.%s", t.path.schema, t.path.table)
}

func (t *table) out() {
	if _, ok := skippable[t.path.table]; ok {
		return
	}
	fmt.Fprintf(os.Stderr, "### %s\n\n", t.uid())
	fmt.Fprintf(os.Stderr, "```\n")
	for _, cn := range t.columns_order {
		t.columns[cn].out()
	}
	fmt.Fprintf(os.Stderr, "```\n")
}

func NewTable(p path) *table {
	return &table{
		path:    p,
		columns: make(map[string]*column),
	}
}

func init() {
	skippable = map[string]bool{
		"goose_db_version": true,
	}
}
