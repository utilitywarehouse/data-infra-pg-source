package parquet

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
	"github.com/utilitywarehouse/data-infra-pg-source/internal/catalog"
)

func New(cat catalog.Catalog) error {
	configSpec := service.NewConfigSpec().
		Summary("Processor for generating parquet files using sql-select input. Expects postgres data types.").
		Field(service.NewStringField("dataProductID").
			Description("Data product id defined in the data-products-definitions").
			Example(uuid.NewString()))

	constructor := func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
		dataProductID, err := conf.FieldString("dataProductID")
		if err != nil {
			return nil, err
		}
		return newParquetProcessor(cat, dataProductID), nil
	}

	err := service.RegisterBatchProcessor("pg_parquet", configSpec, constructor)
	if err != nil {
		return err
	}
	return nil
}

type parquetProcessor struct {
	catalog       catalog.Catalog
	dataProductID string
}

func newParquetProcessor(catalog catalog.Catalog, dataProductID string) *parquetProcessor {
	return &parquetProcessor{
		catalog:       catalog,
		dataProductID: dataProductID,
	}
}

func (r *parquetProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	if len(batch) == 0 {
		return nil, nil
	}

	def, err := r.catalog.GetByID(r.dataProductID)
	if err != nil {
		log.Panicf("Could not find data product with id %v err=%v", r.dataProductID, err)
	}

	schemaDef, err := catalog.ToParquetSchema(*def)
	if err != nil {
		log.Panic(err)
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
		payload, ok := str.([]interface{})
		if !ok {
			log.Panicf("Unexpected message type")
		}
		for _, part := range payload {
			p, ok := part.(map[string]interface{})
			if !ok {
				log.Panicf("Unexpected message type")
			}

			dpPayload := make(map[string]interface{})

			for _, dp := range def.DataProduct.DataPoints {

				dpv, ok := p[dp.Name]
				if (!ok || dpv == nil) && !dp.Optional {
					log.Panicf("Missing required data point %v", dp.Name)
				}
				if !ok || dpv == nil {
					continue
				}
				pqv, err := validateAndConvertToParquetType(dpv, dp.Type)
				if err != nil {
					log.Panic(fmt.Errorf("data point %v err=(%v)", dp.Name, err))
				}
				dpPayload[dp.Name] = pqv
			}

			if err := fw.AddData(dpPayload); err != nil {
				log.Panicf("Error writing to parquet format %v", err)
			}
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

func validateAndConvertToParquetType(in interface{}, destType string) (interface{}, error) {
	switch destType {
	case "BOOLEAN":
		cv, ok := in.(bool)
		if !ok {
			return nil, newValidationError(in, destType)
		}
		return cv, nil
	case "INT64":
		cv, ok := in.(int64)
		if !ok {
			return nil, newValidationError(in, destType)
		}
		return cv, nil
	case "INT32":
		cv, ok := in.(int64)
		if !ok {
			return nil, newValidationError(in, destType)
		}
		return int32(cv), nil
	case "INT":
		cv, ok := in.(int64)
		if !ok {
			return nil, newValidationError(in, destType)
		}
		return int32(cv), nil
	// float32
	case "FLOAT":
		cv, ok := in.(float64)
		if !ok {
			return nil, newValidationError(in, destType)
		}
		return float32(cv), nil
	// float64
	case "DOUBLE":
		cv, ok := in.(float64)
		if !ok {
			return nil, newValidationError(in, destType)
		}
		return cv, nil
	case "UUID":
		cv, ok := in.(string)
		if !ok {
			return nil, newValidationError(in, destType)
		}
		_, err := uuid.Parse(cv)
		if err != nil {
			return nil, newValidationError(in, destType)
		}
		return []byte(cv), nil
	case "STRING":
		cv, ok := in.(string)
		if !ok {
			return nil, newValidationError(in, destType)
		}
		return []byte(cv), nil
	case "TIMESTAMP":
		cv, ok := in.(time.Time)
		if !ok {
			return nil, newValidationError(in, destType)
		}
		return cv.UnixMilli(), nil
	case "DATE":
		cv, ok := in.(time.Time)
		if !ok {
			return nil, newValidationError(in, destType)
		}
		return int32(time.Duration(cv.UnixNano()) / time.Hour / 24), nil
	case "DECIMAL":
		cv, ok := in.(string)
		if !ok {
			return nil, newValidationError(in, destType)
		}
		cvd, err := decimal.NewFromString(cv)
		if err != nil {
			return nil, newValidationError(in, destType)
		}
		if cvd.Exponent() != -2 {
			return nil, newValidationError(in, destType)
		}
		return cvd.CoefficientInt64(), nil
	case "BYTE_ARRAY":
		cv, ok := in.(string)
		if !ok {
			return nil, newValidationError(in, destType)
		}
		return []byte(cv), nil
	default:
		return nil, fmt.Errorf("unknown kind %s is unsupported", destType)
	}
}

func newValidationError(in interface{}, destType string) error {
	return fmt.Errorf("Could not convert %v of type %T to %v", in, in, destType)

}
