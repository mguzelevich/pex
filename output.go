package pex

import (
	"bytes"
	"sort"
	//"fmt"
	//"os"
	"text/template"
)

var markdownTmpl = `{{range .}}# DATABASE {{.Name}}

{{range .Schemas}}## SCHEMA [{{.Name}}]

{{range .Tables}}### {{.Name}}

` + "```" + `
{{range .Columns}}{{if .Pk}}[PK] {{else}}     {{end}}{{.Name}} {{.Type}}{{if .Nullable}} NULL{{end}}{{if .Fks}} ->{{range .Fks}} {{.RefUID}}{{end}}{{end}}
{{end}}
` + "```" + `

{{end}}{{end}}{{end}}`

var dotTmpl = `digraph db {
  rankdir=LR;
  nodesep = 2;
  edge [color=gray50, fontname=Calibri, fontsize=11];
  node [shape=record, fontname=Calibri, fontsize=11];
  splines=true;
  model=subset;
{{range $di, $db := .}}

  subgraph cluster_{{$db.UID}} {
    label = "{{.Name}}";
{{range $si, $s := .Schemas}}{{range $ti, $t := .Tables}}
    {{$t.UID}} [label="{{.Name}} | {{"{{"}} {{.ColumnNames}} {{"}"}} | {{"{"}} {{.ColumnTypes}} {{"}}"}}"];
    {{range .Columns}}{{if .Fks}}{{range $fi, $f := .Fks}}{{$t.UID}} -> {{$f.Table.UID}} [label="{{$f.RefUID}}"]{{end}};
{{end}}{{end}}{{end}}
{{end}}
  }{{end}}

}
`

type Named interface {
	Name() string
}

func Out(format string, dbs Databases) string {
	tmplString := "unknown output format"
	switch format {
	case "markdown":
		tmplString = markdownTmpl
	case "dot":
		tmplString = dotTmpl
	}

	tmpl, err := template.New("databases").Parse(tmplString)
	if err != nil {
		panic(err)
	}

	sort.Sort(dbs)

	output := bytes.Buffer{}
	if err = tmpl.Execute(&output, dbs); err != nil {
		panic(err)
	}
	return output.String()
}
