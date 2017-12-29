package pex

import (
	"fmt"
	"os"
)

type schema struct {
	name   string
	tables map[string]*table
}

func (s *schema) table(p path) *table {
	return s.tables[p.table]
}

func (s *schema) getOrAddTable(p path) *table {
	if _, ok := s.tables[p.table]; !ok {
		s.tables[p.table] = NewTable(p)
	}
	return s.tables[p.table]
}

func (s *schema) out() {
	fmt.Fprintf(os.Stderr, "## SCHEMA [%s]\n\n", s.name)

	for _, t := range s.tables {
		t.out()
	}
}

func NewSchema(p path) *schema {
	return &schema{
		name:   p.schema,
		tables: make(map[string]*table),
	}
}
