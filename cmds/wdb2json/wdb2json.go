package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"strconv"

	"github.com/chamot1111/waldb/fileop"
	"github.com/chamot1111/waldb/tablepacked"
	"github.com/chamot1111/waldb/wutils"
)

func main() {
	var tableDescriptorJSONPath string
	var crc bool
	flag.StringVar(&tableDescriptorJSONPath, "table", "", "path to the json table file descriptor")
	flag.BoolVar(&crc, "crc", false, "read crc corrupted file")
	flag.Parse()

	var tableDescr *tablepacked.Table
	if tableDescriptorJSONPath != "" {
		var err error
		tableDescr, err = tablepacked.UnmarshallJSONTableDescriptor(tableDescriptorJSONPath)
		if err != nil {
			log.Fatalf("could not read table descriptor file: %s", err.Error())
		}
	}

	files := flag.Args()

	rowDataPool := tablepacked.NewRowDataPool()
	bufferPool := tablepacked.NewBufPool()

	for _, p := range files {
		f, err := os.Open(p)
		if err != nil {
			log.Fatalf("could not open file %s: %s", p, err.Error())
		}
		buffer := &wutils.Buffer{}
		err = fileop.GetFileBufferFromFile(f, buffer)
		if err != nil {
			log.Fatalf("could not get file buffer from file %s: %s", p, err.Error())
		}
		buffer.ResetRead()

		err = f.Close()
		if err != nil {
			log.Fatalf("could not close file %s: %s", p, err.Error())
		}

		table, err := tablepacked.ReadAllRowDataFromFileBuffer(buffer, rowDataPool)
		if err != nil {
			if _, ok := err.(*tablepacked.ErrBadEndingCRC); !ok {
				log.Fatalf("could not parse file %s: %s", p, err.Error())
			} else {
				if !crc {
					log.Fatalf("could not parse file %s due to crc problem (use -crc=true to force read): %s", p, err.Error())
				}
			}
		}

		if tableDescr != nil {
			buf, err := tablepacked.RowsDataToJSON(table.Data, *tableDescr, bufferPool)
			if err != nil {
				log.Fatalf("could not export json for file %s: %s", p, err.Error())
			}

			buf.ResetRead()

			log.Println(buf.String())
		} else {
			rowsObjects := make([]map[string]string, 0)
			for _, r := range table.Data {
				obj := make(map[string]string)
				for ic, c := range r.Data {
					obj[strconv.Itoa(ic)] = c.DebugStringRaw()
				}
				rowsObjects = append(rowsObjects, obj)
			}

			content, err := json.Marshal(rowsObjects)
			if err != nil {
				log.Fatalf("could not marshall file %s to json: %s", p, err.Error())
			}

			log.Println(string(content))
		}
	}

}
