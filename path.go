package pex

import (
	"bytes"
	"fmt"
	"strings"
)

type path struct {
	database string
	schema   string
	table    string
	column   string
}

func (p *path) Schema() *path {
	return &path{
		database: p.database,
		schema:   p.schema,
		table:    p.table,
	}
}

func (p *path) Table() *path {
	return &path{
		database: p.database,
		schema:   p.schema,
		table:    p.table,
	}
}

func (p *path) uid() string {
	uid := bytes.Buffer{}
	fmt.Fprintf(&uid, p.database)
	if p.schema != "" {
		fmt.Fprintf(&uid, ".%s", p.schema)
	}
	if p.table != "" {
		fmt.Fprintf(&uid, ".%s", p.table)
	}
	if p.column != "" {
		fmt.Fprintf(&uid, ".%s", p.column)
	}
	return uid.String()
}

func (p *path) UID() string {
	uid := strings.Replace(p.uid(), ".", "_", -1)
	return uid
}

func (p *path) RefUID() string {
	return p.uid()
}
