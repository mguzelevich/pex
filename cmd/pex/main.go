package main

import (
	"bufio"
	"fmt"
	"os"

	_ "github.com/lib/pq"

	"github.com/mguzelevich/pex"
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

func wrapSchemas(schemas []string) []string {
	result := []string{}
	for _, s := range schemas {
		result = append(result, fmt.Sprintf("'%s'", s))
	}
	return result
}

func main() {
	databases := []*pex.Database{}
	if connections, err := readData(); err != nil {
		fmt.Fprintf(os.Stderr, "err: %v\n", err)
		os.Exit(1)
	} else {
		for i, conn := range connections {
			if tmp := fmt.Sprintf("%03d [%s]\n", i, conn); tmp == "" {
				fmt.Fprintf(os.Stderr, "%03d [%s]\n", i, conn)
			}

			d := pex.NewDatabase(conn)
			databases = append(databases, d)
		}
	}

	for i, db := range databases {
		if err := db.Serve(); err != nil {
			fmt.Fprintf(os.Stderr, "%d err: %v\n", i, err)
		} else {
			db.Out()
		}
	}
}
