# HBase REST Dialer 0.1.0
> HBase REST Client GO package

tested in HBase 1.3.1

```
import (
  "../hbase-rest-dialer"
)

func main() {
  hbaseM := hbase.NewDialer("http://localhost:8880")

  err := hbaseM.Namespace.New("msg")
  if err != nil {
    log.Fatal("HBase error: " + err.Error())
  }

  namespaces, err := hbaseM.Namespace.GetAll()
	if err != nil {
		log.Fatal("HBase error: " + err.Error())
	}
  fmt.Println(namespaces)

	// check msg:group
	regions, err := hbaseM.Table.GetRegions("msg:group")
	if err != nil {
		log.Fatal("HBase error: " + err.Error())
	}
  // check table exist or not
	if len(regions) == 0 {
		err = hbaseM.Table.New("msg:group", []string{"from", "content"})
		if err != nil {
			log.Fatal("HBase error: " + err.Error())
		}
	}

  // PUT DATA
  err = hbaseM.Row.Put("msg:group", [][]string{
		[]string{
			"1234566", //rowkey
			"from", "ABC",
      "content", "defghj1235",
		},
	})

  // SCAN DATA
  scanner := hbase.Scanner{
		Batch:    10,
		StartRow: "1234566",
		EndRow:   "1234576",
	}

	rowdata, err := hbaseM.Row.Scan("msg:group", scanner)
	F.IsErr(err)

  fmt.Printf("%v\n", rowdata)
}
```