package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	_ "github.com/lib/pq"

	"github.com/mguzelevich/pex"
)

var (
	outputFormat string
)

func readData() ([]string, error) {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return nil, fmt.Errorf("os.Stdin.Stat() %v", err)
	}

	if fi.Mode()&os.ModeNamedPipe == 0 {
		return nil, fmt.Errorf("no pipe!")
	}

	scanner := bufio.NewScanner(os.Stdin)
	result := []string{}
	for scanner.Scan() { // internally, it advances token based on sperator
		line := scanner.Text()
		result = append(result, line)
	}
	return result, nil
}

func extractDbName(conn string) string {
	dbname := ""
	for _, param := range strings.Split(conn, " ") {
		pair := strings.Split(param, "=")
		if pair[0] == "dbname" {
			dbname = pair[1]
			break
		}
	}
	return dbname
}

func wrapSchemas(schemas []string) []string {
	result := []string{}
	for _, s := range schemas {
		result = append(result, fmt.Sprintf("'%s'", s))
	}
	return result
}

func init() {
	flag.StringVar(&outputFormat, "f", "markdown", "output format (markdown, dot, ...)")
	flag.StringVar(&outputFormat, "out-format", "markdown", "output format (markdown, dot, ...)")
}

func main() {
	flag.Parse()

	databases := pex.Databases{}
	extractors := map[*pex.Extractor]*pex.Database{}
	if connections, err := readData(); err != nil {
		fmt.Fprintf(os.Stderr, "err: %v\n", err)
		os.Exit(1)
	} else {
		for i, conn := range connections {
			if tmp := fmt.Sprintf("%03d [%s]\n", i, conn); tmp == "" {
				fmt.Fprintf(os.Stderr, "%03d [%s]\n", i, conn)
			}
			extractors[pex.NewExtractor(conn)] = pex.NewDatabase(extractDbName(conn))
		}
	}

	i := 1
	for e, db := range extractors {
		if err := e.Serve(db); err != nil {
			fmt.Fprintf(os.Stderr, "%d err: %v\n", i, err)
		} else {
			databases = append(databases, db)
		}
		i++
	}

	fmt.Fprintf(os.Stdout, pex.Out(outputFormat, databases))
}
