package pex

import (
	"sort"
	"strings"
	// "bytes"
	// "fmt"
	// "os"
)

type Database struct {
	Name    string
	schemas map[string]*schema
}

func (d *Database) UID() string {
	uid := strings.Replace(d.Name, " ", "_", -1)
	return uid
}

func (d *Database) Schemas() []*schema {
	schemas := schemas{}
	for _, s := range d.schemas {
		if s.renderable() {
			schemas = append(schemas, s)
		}
	}
	sort.Sort(schemas)
	return schemas
}

func (d *Database) schema(p path) *schema {
	return d.schemas[p.schema]
}

func (d *Database) table(p path) *table {
	return d.schema(p).table(p)
}

func (d *Database) column(p path) *column {
	return d.table(p).column(p)
}

func (d *Database) setPK(ref path) {
	d.column(ref).Pk = true
}

func (d *Database) setFK(fk path, ref path) {
	d.table(fk).AddFk(fk, ref)
}

func (d *Database) getOrAddSchema(p path) *schema {
	name := p.schema
	if _, ok := d.schemas[name]; !ok {
		d.schemas[name] = NewSchema(path{database: p.database, schema: p.schema})
	}
	return d.schemas[name]
}

func NewDatabase(name string) *Database {
	db := &Database{
		Name:    name,
		schemas: make(map[string]*schema),
	}
	return db
}

type Databases []*Database

func (d Databases) Len() int {
	return len(d)
}

func (d Databases) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

func (d Databases) Less(i, j int) bool {
	return d[i].Name < d[j].Name
}
