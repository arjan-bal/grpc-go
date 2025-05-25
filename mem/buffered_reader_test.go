package mem_test

import (
	"bufio"
	"io"
	"strings"
	"testing"

	"google.golang.org/grpc/mem"
)

func TestBufferReader(t *testing.T) {
	input := "abcdefghijklmnopqrstuvwxyz"
	reader := strings.NewReader(input)
	bufReader := mem.NewBufferReader(3, mem.DefaultBufferPool(), reader)
	op1, err := bufReader.Read(2)
	if err != nil {
		t.Fatal(err)
	}
	defer op1.Free()
	if got, want := string(op1.Materialize()), "ab"; got != want {
		t.Fatalf("op1 = %q, want %q", got, want)
	}

	op2, err := bufReader.Read(3)
	if err != nil {
		t.Fatal(err)
	}
	defer op2.Free()
	if got, want := string(op2.Materialize()), "cde"; got != want {
		t.Fatalf("op2 = %q, want %q", got, want)
	}

	op3, err := bufReader.Read(5)
	if err != nil {
		t.Fatal(err)
	}
	defer op3.Free()
	if got, want := string(op3.Materialize()), "fghij"; got != want {
		t.Fatalf("op3 = %q, want %q", got, want)
	}

	got, err := bufReader.Read(100)
	if err == nil {
		t.Fatalf("Got error nil and value %q", string(got.Materialize()))
	}
	t.Log(err)
}

func BenchmarkBufferReader(t *testing.B) {
	input := strings.Repeat("abc", 16_000)
	for range t.N {
		reader := strings.NewReader(input)
		bufReader := mem.NewBufferReader(1024, mem.DefaultBufferPool(), reader)
		for {
			op1, err := bufReader.Read(8)
			if err != nil {
				break
			}
			op1.Materialize()
			op1.Free()
			op2, err := bufReader.Read(1024)
			if err != nil {
				break
			}
			op2.Free()
		}
	}
}

func BenchmarkBufferRegular(t *testing.B) {
	input := strings.Repeat("abc", 16_000)
	for range t.N {
		reader := strings.NewReader(input)
		bufReader := bufio.NewReaderSize(reader, 1024)
		for {
			op1, err := io.ReadFull(bufReader, make([]byte, 8))
			if err != nil {
				break
			}
			if op1 != 8 {
				t.Fatalf("Insufficient bytes: %d", op1)
			}
			op2, err := io.ReadFull(bufReader, *mem.DefaultBufferPool().Get(1024))
			if err != nil {
				break
			}
			if op2 != 1024 {
				t.Fatalf("Insufficient bytes: %d", op2)
			}
		}
	}
}
