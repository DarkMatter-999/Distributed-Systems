package main

import (
	"ddn/mr"
	"log"
	"strconv"
	"strings"
	"time"
	"unicode"
)

func main() {
	/*
		if len(os.Args) != 2 {
			fmt.Fprintf(os.Stderr, "Usage: mrworker \n")
			os.Exit(1)
		}
	*/
	mr.Worker(Map, Reduce)

}

func Map(filename string, contents string) []mr.KeyValue {
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	kva := []mr.KeyValue{}
	for _, w := range words {
		kv := mr.KeyValue{Key: w, Value: "1"}
		kva = append(kva, kv)
	}
	return kva
}

func Reduce(key string, values []string) string {
	// return the number of occurrences of this word.
	return strconv.Itoa(len(values))
}
