// Copyright 2019, 2024 Tamás Gulácsi. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

package giro

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/UNO-SOFT/zlog/v2"

	"github.com/rogpeppe/retry"
	"github.com/tgulacsi/go/iohlp"
	"golang.org/x/net/html"
	"golang.org/x/sync/errgroup"

	"github.com/extrame/xls"
	"github.com/xuri/excelize/v2"
)

//go:embed tabula-*-jar-with-dependencies.jar
var tabulaJar []byte

const DefaultXLSXURL = "https://www.mnb.hu/letoltes/sht.xlsx"
const DefaultURL = "https://www.giro.hu/dokumentumok"
const DefaultPattern = `^(.*-xls-.*$|EHT_([0-9]{8}|[0-9]{4}[_-][0-9]{2}[_-][0-9]{2}|2[0-9]{5})\.(pdf|xlsx?)|AVT_[0-9]{2}_[0-9]{2}_2[0-9]{3}\.(pdf|xlsx?))$`

var ErrNotFound = errors.New("not found")

func SearchXLSURL(ctx context.Context, searchURL, pattern string) (string, error) {
	if searchURL == DefaultXLSXURL {
		return searchURL, nil
	}
	noRedir := http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	req, err := http.NewRequest("GET", searchURL, nil)
	if err != nil {
		return "", fmt.Errorf("%s: %w", searchURL, err)
	}
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return "", fmt.Errorf("%s: %w", searchURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode > 399 {
		var buf strings.Builder
		_, _ = io.Copy(&buf, resp.Body)
		return "", fmt.Errorf("%q: %s: %s", searchURL, resp.Status, buf.String())
	}

	logger := zlog.SFromContext(ctx)

	z := html.NewTokenizer(resp.Body)
	candidates := make([]string, 0, 512)
Loop:
	for {
		tt := z.Next()
		tagName, hasAttr := z.TagName()
		switch tt {
		case html.ErrorToken:
			err := z.Err()
			if errors.Is(err, io.EOF) {
				break Loop
			}
			return "", err

		case html.StartTagToken:
			if hasAttr && bytes.Equal(tagName, []byte("a")) {
				for {
					k, v, more := z.TagAttr()
					if bytes.Equal(k, []byte("href")) {
						if bytes.Contains(v, []byte("/documents/")) && bytes.IndexByte(v, ' ') < 0 {
							candidates = append(candidates, string(v))
						}
					}
					if !more {
						break
					}
				}
			}
		}
	}
	resp.Body.Close()

	strategy := retry.Strategy{Delay: time.Second, MaxDelay: 10 * time.Second, Factor: 1.25, MaxCount: 3}
	rPattern := regexp.MustCompile(pattern)
	resultsCh := make(chan string, 1024)
	errs := make([]error, 0, len(candidates))
	grp, ctx := errgroup.WithContext(ctx)
	grp.SetLimit(8)
	for _, v := range candidates {
		grp.Go(func() error {
			sub, err := url.Parse(v)
			if err != nil {
				logger.Warn("wrong url", "url", string(v), "error", err)
				return nil
			}
			if !(sub.Scheme != "" && sub.IsAbs()) {
				return nil
			}
			u := sub
			logger := logger.With("url", u.String())
			logger.Debug("try")
			if false {
				u := resp.Request.URL.ResolveReference(sub)
				if u.Scheme == "" {
					return nil
				}
			}
			req, err := http.NewRequest("GET", u.String(), nil)
			if err != nil {
				return fmt.Errorf("%s: %w", u.String(), err)
			}
			var resp *http.Response
			for iter := strategy.Start(); ; {
				if resp, err = noRedir.Do(req.WithContext(ctx)); err != nil {
					errs = append(errs, fmt.Errorf("%s: %w", u.String(), err))
				} else {
					break
				}
				if !iter.Next(ctx.Done()) {
					return nil
				}
			}
			defer resp.Body.Close()
			if resp.StatusCode != 302 {
				return nil
			}
			loc := resp.Header.Get("Location")
			bn := path.Base(loc)
			logger.Debug("got", "statusCode", resp.StatusCode, "location", loc)
			if bn == "" {
				return nil
			}
			if rPattern.MatchString(bn) {
				select {
				case resultsCh <- loc:
				default:
				}
			} else if strings.Contains(bn, "EHT") {
				logger.Warn("no match", "pat", rPattern, "loc", loc, "base", bn)
			}
			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		return "", err
	}
	close(resultsCh)
	var results []string
	for s := range resultsCh {
		results = append(results, s)
	}
	if len(results) == 0 {
		return "", fmt.Errorf("%w: %w", ErrNotFound, errors.Join(errs...))
	}
	sort.Slice(results, func(i, j int) bool {
		return path.Base(results[i]) < path.Base(results[j])
	})
	logger.Debug("SearchXLSXURL", "results", results)
	return results[len(results)-1], nil
}

// Parse the reader.
//
// Pass nil as reader to get the default XLSX.
func Parse(ctx context.Context, r io.Reader) ([]Hitelezo, error) {
	if r == nil {
		_, rc, err := DownloadFile(ctx, DefaultXLSXURL)
		if err != nil {
			return nil, err
		}
		defer rc.Close()
		r = rc
	}
	sr, err := iohlp.MakeSectionReader(r, 1<<20)
	if err != nil {
		return nil, err
	}
	var a [1024]byte
	n, err := sr.ReadAt(a[:], 0)
	if err != nil {
		return nil, err
	}
	b := a[:n]
	logger := zlog.SFromContext(ctx)
	//logger.Debug("Parse", "prefix", string(b))
	if bytes.HasPrefix(b, []byte("%PDF-1")) {
		return ParsePDF(ctx, sr)
	}

	hit, err := ParseXLSX(ctx, io.NewSectionReader(sr, 0, sr.Size()))
	logger.Info("ParseXLSX", "hitelezok", len(hit), "error", err)
	if err != nil &&
		(strings.Contains(err.Error(), "not a valid zip") ||
			strings.Contains(err.Error(), "unsupported")) {
		hit, err = ParseXLS(ctx, sr)
	}
	for i := 0; i < len(hit); i++ {
		if hit[i].Bankszerv == "" || hit[i].Nev == "" || (hit[i].Irszam == "" && hit[i].Cim == "") {
			hit[i] = hit[len(hit)-1]
			hit = hit[:len(hit)-1]
			i--
		}
	}
	return hit, err
}
func ParsePDF(ctx context.Context, r io.Reader) ([]Hitelezo, error) {
	logger := zlog.SFromContext(ctx)
	var buf bytes.Buffer
	hit, err := parsePDFTabula(ctx, io.TeeReader(r, &buf))
	logger.Info("parsePDFTabula", "hit", len(hit), "error", err)
	if err == nil {
		return hit, nil
	}

	return parsePDFPdfToText(ctx, io.MultiReader(bytes.NewReader(buf.Bytes()), r))
}

func parsePDFTabula(ctx context.Context, r io.Reader) ([]Hitelezo, error) {
	logger := zlog.SFromContext(ctx)
	logger.Info("ParsePDF tabula")
	dir, err := os.MkdirTemp("", "giro-*")
	if err != nil {
		return nil, fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(dir)
	jarFn := filepath.Join(dir, "tabula.jar")
	if err = os.WriteFile(jarFn, tabulaJar, 0400); err != nil {
		return nil, fmt.Errorf("write jar file: %w", err)
	}
	pdfFh, err := os.Create(filepath.Join(dir, "x.pdf"))
	if err != nil {
		return nil, fmt.Errorf("create temp pdf: %w", err)
	}
	if _, err = io.Copy(pdfFh, r); err != nil {
		return nil, fmt.Errorf("write temp pdf: %w", err)
	}
	if _, err = pdfFh.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("seek %q: %w", pdfFh.Name(), err)
	}
	cmd := exec.CommandContext(ctx, "java", "-jar", jarFn, "-l", "-p", "all", "-f", "CSV", pdfFh.Name())
	cmd.Stdin = pdfFh
	cmd.Stderr = os.Stderr
	pr, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("create stdout pipe: %w", err)
	}
	logger.Debug("start", "args", cmd.Args)
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %v: %w", cmd.Args, err)
	}
	cr := csv.NewReader(pr)
	var hit []Hitelezo
	for {
		row, err := cr.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return hit, fmt.Errorf("read csv: %w", err)
		}
		hit = append(hit, Hitelezo{
			Bankszerv: row[0], Nev: row[1], Irszam: row[2], Cim: row[3],
		})
	}
	return hit, cmd.Wait()
}

func parsePDFPdfToText(ctx context.Context, r io.Reader) ([]Hitelezo, error) {
	logger := zlog.SFromContext(ctx)
	logger.Info("ParsePDF pdftotext")
	cmd := exec.CommandContext(ctx, "pdftotext", "-", "-")
	cmd.Stdin = r
	pr, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("%v: %w", cmd.Args, err)
	}
	hit, err := parseTXT(ctx, pr)
	if waitErr := cmd.Wait(); waitErr != nil {
		if err == nil {
			err = fmt.Errorf("%v: %w", cmd.Args, waitErr)
		}
	}
	return hit, err
}

func parseTXT(ctx context.Context, r io.Reader) ([]Hitelezo, error) {
	logger := zlog.SFromContext(ctx)
	scanner := bufio.NewScanner(r)
	records := make([]Hitelezo, 0, 8192)
	lines := make([]string, 0, 4*32)
	processLines := func() {
		cols := len(lines) / 4
		for i := 0; i < cols; i++ {
			//Log(i, lines[i:i+4])
			h := Hitelezo{
				Bankszerv: lines[0*cols+i], Nev: lines[1*cols+i], Irszam: lines[2*cols+i], Cim: lines[3*cols+i],
			}
			logger.Debug("processLines", "line", lines, "record", h)
			records = checkAppend(records, h)
		}
		lines = lines[:0]
	}
	var numberSeen bool
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		if !numberSeen {
			if numberSeen = '0' <= line[0] && line[0] <= '9'; !numberSeen {
				continue
			}
		}
		//Log("line", string(line))
		if bytes.Contains(line, []byte("nyes Egyszer")) || bytes.HasSuffix(line, []byte(" oldal")) {
			continue
		}
		logger.Debug("", "line", string(line))

		if line[0] == 12 { // Ctrl-L
			processLines()
			rest := line[1:]

			if len(rest) == 0 {
				break
			}
		}
		lines = append(lines, string(bytes.TrimSpace(line)))
	}
	processLines()
	return records, nil
}
func ParseXLSX(ctx context.Context, r io.Reader) ([]Hitelezo, error) {
	logger := zlog.SFromContext(ctx)
	logger.Info("ParseXLSX")
	wb, err := excelize.OpenReader(r)
	if err != nil {
		return nil, err
	}
	rows, err := wb.Rows(wb.GetSheetName(0))
	if err != nil {
		return nil, err
	}
	records := make([]Hitelezo, 0, 8192)
	var headerSkipped, noIrszam bool
	var rec Hitelezo
	dst := []*string{&rec.Bankszerv, &rec.Nev, &rec.Irszam, &rec.Cim}
	for rows.Next() {
		row, err := rows.Columns()
		if err != nil {
			break
		}
		if !headerSkipped {
			headerSkipped = true
			// Branch office code
			// BIC code
			// Name of the branch office
			// Address of the branch office
			// Branch office may send VIBER items
			// Branch office may receive VIBER items
			// logger.Info("header", "row", strings.Join(row, ", "))
			if noIrszam = row[3] == "Address of the branch office"; noIrszam {
				dst = append(dst[:0],
					&rec.Bankszerv, &rec.BIC, &rec.Nev, &rec.Cim)
				// logger.Warn("sht.xlsx", "dst", dst)
			}
			continue
		}
		for j, p := range dst {
			*p = row[j]
		}
		records = checkAppend(records, rec)
		select {
		case <-ctx.Done():
			return records, ctx.Err()
		default:
		}
	}
	logger.Info("ParseXLSX", "records", len(records))
	return records, nil
}

func ParseXLS(ctx context.Context, r io.ReadSeeker) ([]Hitelezo, error) {
	logger := zlog.SFromContext(ctx)
	logger.Info("ParseXLS")
	wb, err := xls.OpenReader(r, "utf8")
	if err != nil {
		logger.Error("xls open", "r", r, "error", err)
		if _, err = r.Seek(0, 0); err != nil {
			return nil, err
		}
		return ParseXLSX(ctx, r)
	}
	sheet := wb.GetSheet(0)
	if sheet == nil {
		return nil, fmt.Errorf("this XLS file does not contain sheet no %d", 0)
	}
	records := make([]Hitelezo, 0, 8192)
	const skip = 1
	var rec Hitelezo
	dst := []*string{&rec.Bankszerv, &rec.Nev, &rec.Irszam, &rec.Cim}
	for n := 0; n < int(sheet.MaxRow); n++ {
		row := sheet.Row(n)
		if n < skip || row == nil {
			continue
		}
		off := row.FirstCol()
		for j, p := range dst {
			*p = row.Col(off + j)
		}
		records = checkAppend(records, rec)
		select {
		case <-ctx.Done():
			return records, ctx.Err()
		default:
		}
	}
	logger.Info("ParseXLS", "records", len(records))
	return records, nil
}

// 10002003	Magyar Államkincstár. értékp.-pénztár	1139	Budapest, Váci út 71.
type Hitelezo struct {
	Bankszerv, BIC, Nev, Irszam, Cim string
}

func (h Hitelezo) String() string {
	return fmt.Sprintf("%s=%q (%s) %s", h.Bankszerv, h.Nev, h.Irszam, h.Cim)
}

func checkAppend(records []Hitelezo, rec Hitelezo) []Hitelezo {
	for _, p := range []*string{&rec.Bankszerv, &rec.Nev, &rec.Irszam, &rec.Cim} {
		*p = strings.TrimSpace(strings.ReplaceAll(*p, "\x00", ""))
	}
	if rec.Irszam == "" && len(rec.Cim) > 5 {
		if i := strings.IndexByte(rec.Cim, ' '); i == 4 &&
			strings.IndexFunc(rec.Cim[:4],
				func(r rune) bool { return !('0' <= r && r <= '9') },
			) < 0 {
			rec.Irszam, rec.Cim = rec.Cim[:4], rec.Cim[5:]
		}
	}
	// fmt.Printf("checkAppend rec=%q\n", rec)
	if rec != (Hitelezo{}) && len(rec.Bankszerv) == 8 {
		records = append(records, rec)
	}
	return records
}

func DownloadFile(ctx context.Context, dlURL string) (string, io.ReadCloser, error) {
	logger := zlog.SFromContext(ctx)
	logger.Info("DownloadFile", "url", dlURL)
	req, err := http.NewRequest("GET", dlURL, nil)
	if err != nil {
		return "", nil, fmt.Errorf("%s: %w", dlURL, err)
	}
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return "", nil, fmt.Errorf("%s: %w", dlURL, err)
	}
	cd := resp.Header.Get("Content-Disposition")
	var filename string
	if _, params, err := mime.ParseMediaType(cd); err == nil {
		filename = params["filename"]
	}
	return filename, resp.Body, nil
}
