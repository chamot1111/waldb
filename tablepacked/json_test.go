package tablepacked

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/chamot1111/waldb/wutils"
)

type c = ColumnDescriptor

// InteractionTable interaction table info
var InteractionTable = Table{
	Name: "interaction",
	Columns: []c{
		{Name: "LearningSessionId", JSONKey: "LearningSessionId", Type: Tuint},
		{Name: "LearningItemId", JSONKey: "LearningItemId", Type: Tuint},
		{Name: "ApplicationId", JSONKey: "ApplicationId", Type: Tuint},
		{Name: "ExerciseId", JSONKey: "ExerciseId", Type: Tuint},
		{Name: "Date", JSONKey: "Date", Type: Tuint},
		{Name: "Type", JSONKey: "Type", Type: Tuint},
		{Name: "TimeSpentOn", JSONKey: "TimeSpentOn", Type: Tuint},
		{Name: "Turn", JSONKey: "Turn", Type: Tuint},
		{Name: "AnswerStatus", JSONKey: "AnswerStatus", Type: Tenum, EnumValues: []string{"none", "ok", "ko", "excluded"}},
		{Name: "AnswerValue", JSONKey: "AnswerValue", Type: Tstring},
	},
}

type interaction struct {
	LearningSessionId int
	LearningItemId    int
	ApplicationId     int
	ExerciseId        int
	Date              int
	Type              int
	TimeSpentOn       int
	Turn              int
	AnswerStatus      string
	AnswerValue       string
}

func cmpInteraction(a, b interaction) bool {
	return a.LearningSessionId == b.LearningSessionId &&
		a.LearningItemId == b.LearningItemId &&
		a.ApplicationId == b.ApplicationId &&
		a.Date == b.Date &&
		a.Type == b.Type &&
		a.TimeSpentOn == b.TimeSpentOn &&
		a.Turn == b.Turn &&
		a.AnswerStatus == b.AnswerStatus &&
		a.AnswerValue == b.AnswerValue
}

func parseInteraction(buf []byte) ([]interaction, error) {
	res := make([]interaction, 0)
	err := json.Unmarshal(buf, &res)
	return res, err
}

func TestEmptyJSON(t *testing.T) {
	p := createBufPool()
	rowsData := make([]*RowData, 0)
	buf, err := RowsDataToJSON(rowsData, InteractionTable, p)
	bytes := buf.Bytes()
	js := string(bytes)
	if err != nil {
		t.Fatalf("ERR %s", err.Error())
	}
	interactions, err := parseInteraction(bytes)
	if err != nil {
		t.Fatalf("ERR %s", err.Error())
	}
	if len(interactions) != 0 {
		t.Fatalf("bad interaction count:%s", js)
	}
}

func Test1InterJSON(t *testing.T) {
	p := createBufPool()
	rowsData := make([]*RowData, 0)
	v := interactionRnd()
	rowsData = append(rowsData, &v)
	buf, err := RowsDataToJSON(rowsData, InteractionTable, p)
	bytes := buf.Bytes()
	js := string(bytes)
	if err != nil {
		t.Fatalf("ERR %s", err.Error())
	}
	interactions, err := parseInteraction(bytes)
	if err != nil {
		t.Fatalf("ERR %s", err.Error())
	}
	if len(interactions) != 1 {
		t.Fatalf("bad interaction count:%s", js)
	}
	if !cmpInteraction(rowDataToInteraction(v), interactions[0]) {
		t.Fatalf("interaction diff:%s", js)
	}
}

func TestNInterJSON(t *testing.T) {
	p := createBufPool()
	const interCount = 100
	rowsData := make([]*RowData, 0)
	for i := 0; i < interCount; i++ {
		v := interactionRnd()
		rowsData = append(rowsData, &v)
	}
	buf, err := RowsDataToJSON(rowsData, InteractionTable, p)
	bytes := buf.Bytes()
	js := string(bytes)
	if err != nil {
		t.Fatalf("ERR %s", err.Error())
	}
	interactions, err := parseInteraction(bytes)
	if err != nil {
		t.Fatalf("ERR %s", err.Error())
	}
	if len(interactions) != interCount {
		t.Fatalf("bad interaction count:%s", js)
	}
	for i := 0; i < interCount; i++ {
		if !cmpInteraction(rowDataToInteraction(*rowsData[i]), interactions[i]) {
			t.Fatalf("interaction diff:%s", js)
		}
	}
}

func createBufPool() *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
			return new(wutils.Buffer)
		},
	}
}

func BenchmarkNInterJSON(b *testing.B) {
	//b.SetParallelism(100)
	p := createBufPool()

	const interCount = 1000

	rowsData := make([]*RowData, 0)
	for i := 0; i < interCount; i++ {
		v := interactionRnd()
		rowsData = append(rowsData, &v)
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {

			buf, err := RowsDataToJSON(rowsData, InteractionTable, p)
			//bytes := buf.Bytes()
			//js := string(bytes)
			if err != nil {
				b.Fatalf("ERR %s", err.Error())
			}
			// interactions, err := parseInteraction(bytes)
			// if err != nil {
			// 	b.Fatalf("ERR %s", err.Error())
			// }
			// if len(interactions) != interCount {
			// 	b.Fatalf("bad interaction count:%s", js)
			// }
			// for i := 0; i < interCount; i++ {
			// 	if !cmpInteraction(rowDataToInteraction(*rowsData[i]), interactions[i]) {
			// 		b.Fatalf("interaction diff:%s", js)
			// 	}
			// }
			p.Put(buf)
		}
	})

}

func rowDataToInteraction(r RowData) interaction {
	return interaction{
		LearningSessionId: int(r.Data[0].EncodedRawValue),
		LearningItemId:    int(r.Data[1].EncodedRawValue),
		ApplicationId:     int(r.Data[2].EncodedRawValue),
		ExerciseId:        int(r.Data[3].EncodedRawValue),
		Date:              int(r.Data[4].EncodedRawValue),
		Type:              int(r.Data[5].EncodedRawValue),
		TimeSpentOn:       int(r.Data[6].EncodedRawValue),
		Turn:              int(r.Data[7].EncodedRawValue),
		AnswerStatus:      InteractionTable.Columns[8].EnumValues[r.Data[8].EncodedRawValue],
		AnswerValue:       string(r.Data[9].Buffer),
	}
}

func interactionRnd() RowData {
	return RowData{
		Data: []ColumnData{
			{ // LearningSessionID
				EncodedRawValue: 1111,
			},
			{ // LearningItemID
				EncodedRawValue: 2222,
			},
			{ // ApplicationID
				EncodedRawValue: 3333,
			},
			{ // ApplicationID
				EncodedRawValue: 3443,
			},
			{ // Date
				EncodedRawValue: 44444,
			},
			{ // Type
				EncodedRawValue: 55555,
			},
			{ // TimeSpentOn
				EncodedRawValue: 66666,
			},
			{ // Turn
				EncodedRawValue: 77777,
			},
			{ // AnswerStatus
				EncodedRawValue: 1,
			},
			{ // AnswerValue
				EncodedRawValue: 15,
				Buffer:          []byte("huguugigiuiugiu"),
			},
		},
	}
}
