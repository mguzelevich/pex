package pex

import (
	// "bytes"
	// "fmt"
	// "os"
	"sort"
)

var renderableSchemas map[string]bool

type schema struct {
	Name   string
	tables map[string]*table
}

func (s *schema) table(p path) *table {
	return s.tables[p.table]
}

func (s *schema) Tables() []*table {
	tbls := tables{}
	for _, t := range s.tables {
		if t.renderable() {
			tbls = append(tbls, t)
		}
	}
	sort.Sort(tbls)
	return tbls
}

func (s *schema) getOrAddTable(p path) *table {
	if _, ok := s.tables[p.table]; !ok {
		s.tables[p.table] = NewTable(path{database: p.database, schema: p.schema, table: p.table})
	}
	return s.tables[p.table]
}

func (s *schema) renderable() bool {
	_, ok := renderableSchemas[s.Name]
	return ok
}

func NewSchema(p path) *schema {
	return &schema{
		Name:   p.schema,
		tables: make(map[string]*table),
	}
}

func init() {
	renderableSchemas = map[string]bool{
		"public": true,
	}
}

type schemas []*schema

func (s schemas) Len() int {
	return len(s)
}

func (s schemas) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s schemas) Less(i, j int) bool {
	return s[i].Name < s[j].Name
}
