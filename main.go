package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
  "strings"
)

type Field struct {
	Name      string
	Number    int
	Type      string
	Attribute string
}

type Message struct {
	Name        string
	Fields      []*Field
	SubMessages []*Message
}

type Protobuf struct {
	Package  string
	Messages []*Message
}

func (f *Field) ParseField(r *TokenReader) error {
	typ, err := r.NextToken()
	if err != nil {
		return err
	}

	f.Type = typ

	name, err := r.NextToken()
	if err != nil {
		return err
	}

	f.Name = name

	eq, err := r.NextToken()
	if err != nil {
		return err
	}

	if eq != "=" {
		return errors.New("expected equals sign after field name")
	}

	fieldnum, err := r.NextToken()
	if err != nil {
		return err
	}

	fnum, err := strconv.Atoi(fieldnum)
	if err != nil {
		return err
	}

	f.Number = fnum

	semi, err := r.NextToken()
	if err != nil {
		return err
	}

	if semi != ";" {
		return errors.New("expected a semicolon after field number")
	}
	return nil
}

func ParseMessage(r *TokenReader) (*Message, error) {
	m := new(Message)
	mesname, err := r.NextToken()
	if err != nil {
		return nil, err
	}

	openBracket, err := r.NextToken()
	if err != nil {
		return nil, err
	}

	if openBracket != "{" {
		return nil, errors.New("expected opening bracket after message name")
	}

	m.Name = mesname

	for {
		tok, err := r.NextToken()
		if err != nil {
			return nil, err
		}

		switch tok {
		case "}":
			return m, nil
		case "repeated", "required", "optional":
			// its a field!
			f := &Field{Attribute: tok}
			err := f.ParseField(r)
			if err != nil {
				return nil, err
			}

			m.Fields = append(m.Fields, f)

		case "message":
			// its a submessage!
			subm, err := ParseMessage(r)
			if err != nil {
				return nil, err
			}

			m.SubMessages = append(m.SubMessages, subm)
		default:
			return nil, fmt.Errorf("Unrecognized token: %s", tok)
		}
	}
}

func ParseProtoFile(r io.Reader) (*Protobuf, error) {
	pb := new(Protobuf)
	read := NewTokenReader(r)
	for {
		tok, err := read.NextToken()
		if err != nil {
			if err == io.EOF {
				return pb, nil
			}
			return nil, err
		}

		switch tok {
		case "package":
			pkgname, err := read.NextToken()
			if err != nil {
				return nil, err
			}
			pb.Package = pkgname
			semi, err := read.NextToken()
			if err != nil {
				return nil, err
			}

			if semi != ";" {
				return nil, errors.New("expected semicolon after package name")
			}
		case "message":
			message, err := ParseMessage(read)
			if err != nil {
				return nil, err
			}

			pb.Messages = append(pb.Messages, message)

		default:
			fmt.Println("Unrecognized token: ", tok)
		}
	}
	return nil, nil
}

func PrintGoStreamProto(w io.Writer, pb *Protobuf) {
	fmt.Printf("package %s\n\n", pb.Package)
	for _, mes := range pb.Messages {
		printGoProtoMessage(w, mes, "", true)
	}
}

func makeGoName(s string) string {
	return strings.ToUpper(s[:1]) + s[1:]
}

var typeMap = map[string]string{
	"string": "string",
	"bytes":  "[]byte",
	"int32":  "int32",
	"uint32": "uint32",
	"int64":  "int64",
	"uint64": "uint64",
	"bool":   "bool",
}

func parseGoType(typ string, prefix string, rep bool) string {
	t, ok := typeMap[typ]
	if ok {
		if rep || t[:2] == "[]" {
			return t
		} else {
			return "*" + t
		}
	}

	return "*" + prefix + makeGoName(typ)
}

func formatGoProtoField(f *Field, prefix string, stream bool) string {
	var typ string
	if f.Attribute == "repeated" {
		if stream {
			typ = "chan "
		} else {
			typ = "[]"
		}
	}
	typ += parseGoType(f.Type, prefix, f.Attribute == "repeated")
	name := makeGoName(f.Name)

	tag := fmt.Sprintf("`protobuf:\"%s,%d,%s,name=%s\"`", f.Type, f.Number, f.Attribute[:3], f.Name)
	return fmt.Sprintf("%s %s %s", name, typ, tag)
}

func printGoProtoMessage(w io.Writer, mes *Message, prefix string, stream bool) {
	name := prefix + mes.Name
	fmt.Fprintf(w, "type %s struct {\n", name)
	for _, f := range mes.Fields {
		fmt.Fprintln(w, "\t"+formatGoProtoField(f, name+"_", stream))
	}
	if stream {
		fmt.Fprintln(w, "\terrors chan error")
		fmt.Fprintln(w, "\tcloseCh chan struct{}")
	}
	fmt.Fprintln(w, "}\n")

	printMessageConstructor(w, mes, name, stream)

	if stream {
		printGoStreamMethods(w, mes, name)
	}
	printProtoMethods(w, name)
	printInterfaceAssertion(w, mes, name, stream)

	for _, subm := range mes.SubMessages {
		printGoProtoMessage(w, subm, name+"_", false)
	}
}

func printInterfaceAssertion(w io.Writer, mes *Message, name string, stream bool) {
	var typ string
	if stream {
		typ = "pbs.StreamMessage"
	} else {
		typ = "proto.Message"
	}
	fmt.Fprintf(w, "var _ %s = (*%s)(nil)\n\n", typ, name)
}

func printGoStreamMethods(w io.Writer, mes *Message, name string) {
	fmt.Fprintf(w, "func (m *%s) Errors() chan error { return m.errors }\n\n", name)
	fmt.Fprintf(w, "func (m *%s) Closed() <-chan struct{} { return m.closeCh }\n\n", name)
	fmt.Fprintf(w, "func (m *%s) Close() error {\n", name)
	for _, f := range mes.Fields {
		if f.Attribute == "repeated" {
			fmt.Fprintf(w, "\tclose(m.%s)\n", makeGoName(f.Name))
		}
	}
	fmt.Fprintf(w, "\tclose(m.errors)\n")
	fmt.Fprintf(w, "\tclose(m.closeCh)\n")
	fmt.Fprintln(w, "\treturn nil")
	fmt.Fprintln(w, "}\n")
}

func printMessageConstructor(w io.Writer, mes *Message, name string, stream bool) {
	fmt.Fprintf(w, "func New%s() *%s {\n", name, name)
	fmt.Fprintf(w, "\treturn &%s{\n", name)
	if stream {
		fmt.Fprintf(w, "\t\terrors: make(chan error, 1),\n")
		fmt.Fprintf(w, "\t\tcloseCh: make(chan struct{}),\n")
		for _, f := range mes.Fields {
			if f.Attribute == "repeated" {
				fmt.Fprintf(w, "\t\t%s: make(chan %s),\n", makeGoName(f.Name), parseGoType(f.Type, name+"_", true))
			}
		}
	}
	fmt.Fprintf(w, "\t}\n}\n")
}

func printProtoMethods(w io.Writer, name string) {
	fmt.Fprintf(w, "func (*%s) ProtoMessage() {}\n\n", name)
	fmt.Fprintf(w, "func (m *%s) String() string {return proto.CompactTextString(m)}\n\n", name)
	fmt.Fprintf(w, "func (m *%s) Reset() {*m = *New%s()}\n\n", name, name)
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("specify protobuf file to compile")
		return
	}
	arg := os.Args[1]
	file, err := os.Open(arg)
	if err != nil {
		fmt.Println(err)
		return
	}
	proto, err := ParseProtoFile(file)
	if err != nil {
		fmt.Println(err)
		return
	}
	PrintGoStreamProto(os.Stdout, proto)
}
