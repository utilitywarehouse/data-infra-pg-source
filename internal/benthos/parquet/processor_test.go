package parquet

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/benthosdev/benthos/v4/public/service"
	goparquet "github.com/fraugster/parquet-go"
	_ "github.com/lib/pq"
	"github.com/utilitywarehouse/data-products-definitions/pkg/catalog/v1"
)

const expectedCitizenID = "75d44fdc-dffd-42ea-af06-06fa4cb6fdbd"

func TestParquetProcessor(t *testing.T) {
	proc := newParquetProcessor(catalog.New("../../../testassets/datadefinitions"), "75d44fdc-dffd-42ea-af06-06fa4cb6fdbd", service.MockResources().Logger())
	result, err := proc.ProcessBatch(context.Background(), service.MessageBatch{service.NewMessage([]byte(fmt.Sprintf(`{"citizen_id": "%s"}`, expectedCitizenID)))})
	if err != nil {
		t.Fatalf("Unexpected error %v", err)
	}

	if len(result) == 0 || len(result[0]) == 0 {
		t.Fatalf("Processing result should not be empty")
	}
	payload, err := result[0][0].AsBytes()
	if err != nil {
		t.Fatalf("Unexpected error %v", err)
	}

	fr, err := goparquet.NewFileReader(bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("Unexpected error %v", err)
	}

	row, err := fr.NextRow()
	if err == io.EOF {
		t.Fatalf("Parquet file should have one row")
	}

	citizenID, ok := row["citizen_id"]
	if !ok {
		t.Fatalf("Row should contain citizen_id")
	}
	if expectedCitizenID != fmt.Sprintf("%s", citizenID) {
		t.Fatalf("Expected %s got %s", expectedCitizenID, citizenID)
	}
}

func TestParquetProcessorValidationError(t *testing.T) {
	proc := newParquetProcessor(catalog.New("../../../testassets/datadefinitions"), "75d44fdc-dffd-42ea-af06-06fa4cb6fdbd", service.MockResources().Logger())
	_, err := proc.ProcessBatch(context.Background(), service.MessageBatch{service.NewMessage([]byte(`{"citizen_id": "asdf"}`))})
	if err == nil {
		t.Fatal("Expected error")
	}
}

func TestParquetProcessorMissingMandatoryField(t *testing.T) {
	proc := newParquetProcessor(catalog.New("../../../testassets/datadefinitions"), "75d44fdc-dffd-42ea-af06-06fa4cb6fdbd", service.MockResources().Logger())
	_, err := proc.ProcessBatch(context.Background(), service.MessageBatch{service.NewMessage([]byte(`{"random": "asdf"}`))})
	if err == nil {
		t.Fatal("Expected error")
	}
}
