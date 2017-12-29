package pex

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"strings"
)

var schemas map[string]bool

type path struct {
	schema string
	table  string
	column string
}

func (p *path) uid() string {
	uid := bytes.Buffer{}
	fmt.Fprintf(&uid, p.schema)
	if p.table != "" {
		fmt.Fprintf(&uid, ".%s", p.table)
	}
	if p.column != "" {
		fmt.Fprintf(&uid, ".%s", p.column)
	}
	return uid.String()
}

type Database struct {
	name    string
	conn    string
	db      *sql.DB
	schemas map[string]*schema
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
	d.column(ref).pk = true
}

func (d *Database) setFK(fk path, ref path) {
	d.table(fk).AddFk(fk, ref)
}

func (d *Database) getOrAddSchema(p path) *schema {
	name := p.schema
	if _, ok := d.schemas[name]; !ok {
		d.schemas[name] = NewSchema(p)
	}
	return d.schemas[name]
}

func (d *Database) Serve() error {
	//conn := "dbname=ms_auth sslmode=disable"
	if db, err := sql.Open("postgres", d.conn); err != nil {
		fmt.Fprintf(os.Stderr, "err: %v\n", err)
		os.Exit(1)
	} else {
		d.db = db
	}

	d.serveTables()
	d.servePK()
	d.serveFK()
	return nil
}

func (d *Database) serveTables() error {
	q := `
        SELECT
          tables.table_schema, tables.table_name, columns.column_name,
          COALESCE(columns.domain_name, columns.data_type) AS data_type, columns.is_nullable
        FROM
          information_schema.tables tables
          join information_schema.columns columns on columns.table_schema=tables.table_schema and columns.table_name=tables.table_name
        WHERE tables.table_type = 'BASE TABLE'
        ORDER BY tables.table_schema, tables.table_name, columns.ordinal_position
    `
	rows, err := d.db.Query(q)
	if err != nil {
		return err
	}

	for rows.Next() {
		var p path
		var data_type string
		var is_nullable string

		if err := rows.Scan(&p.schema, &p.table, &p.column, &data_type, &is_nullable); err != nil {
			fmt.Fprintf(os.Stderr, "[%s] scan error %v\n", d.conn, err)
			break
		}
		schema := d.getOrAddSchema(p)
		table := schema.getOrAddTable(p)
		table.AddColumn(p, data_type, is_nullable == "YES")
	}
	return nil
}

func (d *Database) servePK() error {
	q := `
        SELECT
          tc.table_schema, tc.table_name, ccu.column_name, tc.constraint_type
        FROM
          information_schema.table_constraints tc
          join information_schema.constraint_column_usage ccu
            on tc.constraint_schema=ccu.constraint_schema
              and tc.constraint_catalog=ccu.constraint_catalog
              and tc.constraint_name=ccu.constraint_name
        WHERE
          tc.constraint_type in ('PRIMARY KEY')
    `
	rows, err := d.db.Query(q)
	if err != nil {
		return err
	}

	for rows.Next() {
		var p path
		var constraint_type string

		if err := rows.Scan(&p.schema, &p.table, &p.column, &constraint_type); err != nil {
			fmt.Fprintf(os.Stderr, "[%s] scan error %v\n", d.conn, err)
			break
		}
		d.setPK(p)
	}
	return nil
}

func (d *Database) serveFK() error {
	q := `
        SELECT
          kcu.table_schema, kcu.table_name, kcu.column_name,
          ccu.table_schema, ccu.table_name, ccu.column_name
        FROM
          information_schema.table_constraints tc
          left join information_schema.constraint_column_usage ccu
            on tc.constraint_schema=ccu.constraint_schema
              and tc.constraint_catalog=ccu.constraint_catalog
              and tc.constraint_name=ccu.constraint_name
          left join information_schema.key_column_usage kcu
            on tc.constraint_schema=kcu.constraint_schema
              and tc.constraint_catalog=kcu.constraint_catalog
              and tc.constraint_name=kcu.constraint_name
        WHERE
          tc.constraint_type in ('FOREIGN KEY')
    `
	rows, err := d.db.Query(q)
	if err != nil {
		return err
	}

	for rows.Next() {
		var fk path
		var ref path

		if err := rows.Scan(
			&fk.schema, &fk.table, &fk.column,
			&ref.schema, &ref.table, &ref.column,
		); err != nil {
			fmt.Fprintf(os.Stderr, "[%s] scan error %v\n", d.conn, err)
			break
		}
		d.setFK(fk, ref)
	}
	return nil
}

func (d *Database) Out() {
	fmt.Fprintf(os.Stderr, "# DATABASE [%s]\n\n", d.name)

	for _, s := range d.schemas {
		if _, ok := schemas[s.name]; ok {
			s.out()
		}
	}
}

func NewDatabase(conn string) *Database {
	db := &Database{
		conn:    conn,
		schemas: make(map[string]*schema),
	}

	p := strings.Split(conn, " ")
	for _, pp := range p {
		ppp := strings.Split(pp, "=")
		if ppp[0] == "dbname" {
			db.name = ppp[1]
		}
	}

	return db
}

func init() {
	schemas = map[string]bool{
		"public": true,
	}
}
