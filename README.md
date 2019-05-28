# PEx [![GoDoc](https://godoc.org/github.com/mguzelevich/pex?status.svg)](http://godoc.org/github.com/mguzelevich/pex) [![Build Status](https://travis-ci.org/mguzelevich/pex.svg?branch=master)](https://travis-ci.org/mguzelevich/pex)

postgres schema exporter


## instalation

```
go get -u github.com/mguzelevich/pex/cmd/...
```

## build

```
go build github.com/mguzelevich/pex/cmd/...
```

## usage

```
$ echo "dbname=<DBNAME> sslmode=disable" | ./pex
```

```
$ echo "dbname=<DBNAME>  user=<USER> password=<PASSWD> host=<HOST> sslmode=disable" | ./pex
```

```
$ echo "dbname=<DBNAME> sslmode=disable" | ./pex -f dot | dot -Tpng > /tmp/databases.png && pqiv /tmp/databases.png
```
