// Copyright 2019, 2024 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package giro

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseDefault(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	hit, err := Parse(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(hit)
	t.Logf("%s", string(b))
	if len(hit) == 0 {
		t.Error("hit=nil")
	}
}

func TestParsePDF(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fh, err := os.Open(filepath.Join("testdata", "EHT_20210401.txt"))
	if err == nil {
		hs, err := parseTXT(ctx, fh)
		fh.Close()
		if err != nil {
			t.Error(err)
		}
		checkHs(t, hs)
	}

	fh, err = os.Open(filepath.Join("testdata", "EHT_20210401.pdf"))
	if err == nil {
		hs, err := ParsePDF(ctx, fh)
		fh.Close()
		if err != nil {
			t.Error(err)
		}
		checkHs(t, hs)
	}
}

func checkHs(t *testing.T, hs []Hitelezo) {
	t.Log(len(hs))
	const wanted = 100
	if len(hs) == 0 {
		t.Fatalf("got %d records, wanted %d", len(hs), wanted)
	}
	for i, h := range hs {
		if len(h.Bankszerv) != 8 {
			t.Errorf("%d. bankszerv=%q", i, h.Bankszerv)
		}
		if len(h.Irszam) != 4 {
			t.Errorf("%d. irszam=%q", i, h.Irszam)
		}
	}
}
