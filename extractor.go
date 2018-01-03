package pex

import (
	"database/sql"
	"fmt"
	"os"
)

type Extractor struct {
	conn string
	db   *sql.DB
}

func (e *Extractor) Serve(database *Database) error {
	e.serveTables(database)
	e.servePK(database)
	e.serveFK(database)
	return nil
}

func (e *Extractor) serveTables(database *Database) error {
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
	rows, err := e.db.Query(q)
	if err != nil {
		return err
	}

	for rows.Next() {
		var data_type string
		var is_nullable string

		columnPath := path{
			database: database.Name,
		}

		if err := rows.Scan(&columnPath.schema, &columnPath.table, &columnPath.column, &data_type, &is_nullable); err != nil {
			fmt.Fprintf(os.Stderr, "[%s] scan error %v\n", e.conn, err)
			break
		}
		schema := database.getOrAddSchema(columnPath)
		table := schema.getOrAddTable(columnPath)
		table.AddColumn(columnPath, data_type, is_nullable == "YES")
	}
	return nil
}

func (e *Extractor) servePK(database *Database) error {
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
	rows, err := e.db.Query(q)
	if err != nil {
		return err
	}

	for rows.Next() {
		var constraint_type string

		p := path{
			database: database.Name,
		}

		if err := rows.Scan(&p.schema, &p.table, &p.column, &constraint_type); err != nil {
			fmt.Fprintf(os.Stderr, "[%s] scan error %v\n", e.conn, err)
			break
		}
		database.setPK(p)
	}
	return nil
}

func (e *Extractor) serveFK(database *Database) error {
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
	rows, err := e.db.Query(q)
	if err != nil {
		return err
	}

	for rows.Next() {
		fk := path{
			database: database.Name,
		}
		ref := path{
			database: database.Name,
		}

		if err := rows.Scan(
			&fk.schema, &fk.table, &fk.column,
			&ref.schema, &ref.table, &ref.column,
		); err != nil {
			fmt.Fprintf(os.Stderr, "[%s] scan error %v\n", e.conn, err)
			break
		}
		database.setFK(fk, ref)
	}
	return nil
}

func NewExtractor(conn string) *Extractor {
	e := &Extractor{
		conn: conn,
	}
	if db, err := sql.Open("postgres", conn); err != nil {
		fmt.Fprintf(os.Stderr, "err: %v\n", err)
		os.Exit(1)
	} else {
		e.db = db
	}
	return e
}
