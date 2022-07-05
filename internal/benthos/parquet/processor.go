package parquet

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/benthosdev/benthos/v4/public/service"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/google/uuid"
	"github.com/utilitywarehouse/data-products-definitions/pkg/catalog/v1"
)

func New(cat catalog.Catalog) error {
	configSpec := service.NewConfigSpec().
		Summary("Processor for generating parquet files using sql_raw input.").
		Field(service.NewStringField("dataProductID").
			Description("Data product id defined in the data-products-definitions").
			Example(uuid.NewString()))

	constructor := func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
		dataProductID, err := conf.FieldString("dataProductID")
		if err != nil {
			return nil, err
		}
		return newParquetProcessor(cat, dataProductID, mgr.Logger()), nil
	}

	err := service.RegisterBatchProcessor("uw_parquet", configSpec, constructor)
	if err != nil {
		return err
	}
	return nil
}

type parquetProcessor struct {
	catalog       catalog.Catalog
	dataProductID string
	logger        *service.Logger
}

func newParquetProcessor(catalog catalog.Catalog, dataProductID string, logger *service.Logger) *parquetProcessor {
	return &parquetProcessor{
		catalog:       catalog,
		dataProductID: dataProductID,
		logger:        logger,
	}
}

func (r *parquetProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	r.logger.Infof("Parquet processor: processing batch of size %v", len(batch))
	if len(batch) == 0 {
		return nil, nil
	}

	def, err := r.catalog.GetByID(r.dataProductID)
	if err != nil {
		return nil, fmt.Errorf("Could not find data product with id %v err=%v", r.dataProductID, err)
	}

	schemaDef, err := catalog.ToParquetSchema(*def)
	if err != nil {
		return nil, err
	}

	buf := bytes.Buffer{}
	fw := goparquet.NewFileWriter(&buf,
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithSchemaDefinition(schemaDef),
		goparquet.WithCreator("write-lowlevel"),
	)

	for _, msg := range batch {
		str, err := msg.AsStructured()
		if err != nil {
			return nil, err
		}
		if err := processRow(str, def, fw); err != nil {
			return nil, err
		}

	}
	if err := fw.Close(); err != nil {
		return nil, err
	}
	outMsg := service.NewMessage(buf.Bytes())
	return []service.MessageBatch{{outMsg}}, nil
}

func (r *parquetProcessor) Close(ctx context.Context) error {
	return nil
}

func processRow(row interface{}, def *catalog.Definition, fw *goparquet.FileWriter) error {
	p, ok := row.(map[string]interface{})
	if !ok {
		return fmt.Errorf("Unexpected message type %T", row)
	}

	dpPayload := make(map[string]interface{})

	for _, dp := range def.DataProduct.DataPoints {

		dpv, ok := p[dp.Name]
		if (!ok || dpv == nil) && !dp.Optional {
			return fmt.Errorf("Missing required data point %v", dp.Name)
		}
		if !ok || dpv == nil {
			continue
		}
		if dp.Type == catalog.DPType_Array || dp.Type == catalog.DPType_Object {
			nestedPayload, ok := dpv.(string)
			if !ok {
				return fmt.Errorf("Nested data point %v should be of type jsob", dp.Name)
			}
			var nested interface{}
			if err := json.Unmarshal([]byte(nestedPayload), &nested); err != nil {
				return fmt.Errorf("Nested data point %v should be of type jsob", dp.Name)
			}
			dpv = nested
		}

		pqv, err := catalog.ValidateAndConvertToParquetType(dpv, dp)
		if err != nil {
			return fmt.Errorf("data point %v err=(%v)", dp.Name, err)
		}
		dpPayload[dp.Name] = pqv
	}

	if err := fw.AddData(dpPayload); err != nil {
		return fmt.Errorf("Error writing to parquet format %v", err)
	}
	return nil
}
