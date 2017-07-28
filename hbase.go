package hbase

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"
)

// Dialer ...
type Dialer struct {
	URL       string
	Namespace Namespace
	Table     Table
	Row       Row
}

// Namespace ...
type Namespace struct {
	parent *Dialer
}

// Table ...
type Table struct {
	parent *Dialer
}

// Row ...
type Row struct {
	parent *Dialer
}

// ScanData ...
type ScanData struct {
	Row []RowData `json:"Row"`
}

// RowData ...
type RowData struct {
	Key  string     `json:"key"`
	Cell []CellData `json:"Cell"`
}

// CellData ...
type CellData struct {
	Column    string `json:"column"`
	Timestamp int    `json:"timestamp"`
	Value     string `json:"$"`
}

// Scanner ...
type Scanner struct {
	Batch    int                    `json:"batch,omitempty"`
	StartRow string                 `json:"startRow,omitempty"`
	EndRow   string                 `json:"endRow,omitempty"`
	Filter   map[string]interface{} `json:"filter,omitempty"`
}

// NewDialer ...
func NewDialer(url string) *Dialer {
	d := &Dialer{URL: url}
	d.Namespace.parent = d
	d.Table.parent = d
	d.Row.parent = d
	return d
}

func (d *Dialer) dial(method string, rest string, buf []byte, fullURL bool) (*http.Response, error) {
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	url := rest
	if !fullURL {
		url = d.URL + rest
	}

	var req *http.Request
	if buf == nil {
		req, _ = http.NewRequest(method, url, nil)
	} else {
		req, _ = http.NewRequest(method, url, bytes.NewBuffer(buf))
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")

	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	return res, err
}

func toJSON(body io.ReadCloser, decode bool) (obj map[string]interface{}, err error) {
	err = json.NewDecoder(body).Decode(&obj)
	if err != nil {
		return nil, err
	}
	if decode {
		obj = decodeJSON(obj)
	}
	return obj, nil
}

func fromJSON(obj map[string]interface{}, encode bool) ([]byte, error) {
	if encode {
		obj = encodeJSON(obj)
	}
	buf, err := json.Marshal(obj)
	return buf, err
}

func b64Encode(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

func b64Decode(s string) string {
	decoded, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return "invalid base64"
	}
	return string(decoded)
}

func encodeJSON(obj map[string]interface{}) map[string]interface{} {
	return deepinJSON(obj, true).(map[string]interface{})
}

func decodeJSON(obj map[string]interface{}) map[string]interface{} {
	return deepinJSON(obj, false).(map[string]interface{})
}

func deepinJSON(obj interface{}, encode bool) interface{} {
	if objMap, ok := obj.(map[string]interface{}); ok {
		for k, v := range objMap {
			objMap[k] = deepinJSON(v, encode)
		}
		return objMap
	} else if objSlice, ok := obj.([]interface{}); ok {
		for i, v := range objSlice {
			objSlice[i] = deepinJSON(v, encode)
		}
		return objSlice
	}
	if encode {
		return b64Encode(obj.(string))
	}
	return b64Decode(obj.(string))
}

func (s *Scanner) convert() ([]byte, error) {
	s.StartRow = b64Encode(s.StartRow)
	s.EndRow = b64Encode(s.EndRow)
	buf, err := json.Marshal(s)
	return buf, err
}

func (s *ScanData) convert(body io.ReadCloser) ([]RowData, error) {
	err := json.NewDecoder(body).Decode(s)
	if err != nil {
		return nil, err
	}
	for i, r := range s.Row {
		s.Row[i].Key = b64Decode(r.Key)
		for j, c := range r.Cell {
			s.Row[i].Cell[j].Column = b64Decode(c.Column)
			s.Row[i].Cell[j].Value = b64Decode(c.Value)
		}
	}
	return s.Row, err
}

// GetAll  ...
func (n *Namespace) GetAll() (map[string]interface{}, error) {
	res, err := n.parent.dial("GET", "/namespaces", nil, false)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return nil, errors.New("namespace getall > " + res.Status)
	}
	body, err := toJSON(res.Body, false)
	if err != nil {
		return nil, err
	}
	return body, nil
}

// New ...
func (n *Namespace) New(name string) error {
	res, err := n.parent.dial("POST", "/namespaces/"+name, nil, false)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != 201 {
		return errors.New("namespace new > " + res.Status)
	}
	return nil
}

// GetRegions ...
func (t *Table) GetRegions(name string) ([]interface{}, error) {
	res, err := t.parent.dial("GET", "/"+name+"/regions", nil, false)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return nil, errors.New("table getregions > " + res.Status)
	}
	body, err := toJSON(res.Body, false)
	if err != nil {
		return nil, err
	}
	return body["Region"].([]interface{}), nil
}

// New ...
func (t *Table) New(name string, schema []string) error {
	columns := []interface{}{}
	for _, s := range schema {
		columns = append(columns, map[string]interface{}{"name": s})
	}
	obj := map[string]interface{}{
		"ColumnSchema": columns,
	}
	buf, err := fromJSON(obj, false)
	if err != nil {
		return err
	}
	res, err := t.parent.dial("POST", "/"+name+"/schema", buf, false)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != 201 {
		return errors.New("table new > " + res.Status)
	}
	return nil
}

// Put ...
func (r *Row) Put(table string, datas [][]string) error {
	if len(datas)%2 == 0 {
		return errors.New("row put > datas len is not odd")
	}
	rows := []map[string]interface{}{}
	for _, d := range datas {
		cell := []map[string]interface{}{}
		for i := 1; i < len(d[1:]); i += 2 {
			cell = append(cell, map[string]interface{}{
				"column": b64Encode(d[i]),
				"$":      b64Encode(d[i+1]),
			})
		}
		row := map[string]interface{}{
			"key":  b64Encode(d[0]),
			"Cell": cell,
		}
		rows = append(rows, row)
	}
	obj := map[string]interface{}{
		"Row": rows,
	}
	buf, err := fromJSON(obj, false)
	if err != nil {
		return err
	}
	res, err := r.parent.dial("PUT", "/"+table+"/putrow", buf, false)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return errors.New("row put > " + res.Status)
	}
	return nil
}

// Scan ...
func (r *Row) Scan(table string, scanner Scanner) ([]RowData, error) {
	buf, err := scanner.convert()
	res, err := r.parent.dial("PUT", "/"+table+"/scanner", buf, false)
	if err != nil {
		return nil, errors.New("put scan obj > " + err.Error())
	}
	scanObj := res.Header.Get("Location")
	res.Body.Close()

	if res.StatusCode != 201 {
		return nil, errors.New("put scan obj > " + res.Status)
	}

	res, err = r.parent.dial("GET", scanObj, nil, true)
	if err != nil {
		return nil, errors.New("get scan obj > " + err.Error())
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return nil, errors.New("get scan obj > " + res.Status)
	}

	go func() {
		res, err = r.parent.dial("DELETE", scanObj, nil, true)
		if err == nil {
			res.Body.Close()
		}
	}()

	var result ScanData
	return result.convert(res.Body)
}
