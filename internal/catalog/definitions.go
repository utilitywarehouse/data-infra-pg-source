package catalog

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	"gopkg.in/yaml.v2"
)

var (
	ErrMissingDefinition = errors.New("missing definition")
	ErrNoBrazeMapping    = errors.New("missing definition")
)

type Catalog struct {
	baseDir string
}

type Definition struct {
	DataProduct DataProduct `yaml:"definition"`
}

type DataProduct struct {
	ID          string      `yaml:"id"`
	FQN         string      `yaml:"fqn"`
	Description string      `yaml:"description"`
	UniqueKey   []string    `yaml:"uniqueKey"`
	DataPoints  []DataPoint `yaml:"dataPoints"`
}

type DataPoint struct {
	Name        string   `yaml:"name"`
	Description string   `yaml:"description"`
	Type        string   `yaml:"type"`
	Optional    bool     `yaml:"optional"`
	Egress      *Egress  `yaml:"egress"`
	Depends     []string `yaml:"depends"`
	Tags        []string `yaml:"tags"`
}

type Egress struct {
	Braze *Braze `yaml:"braze"`
}

type Braze struct {
	Identity string `yaml:"identity"`
	Name     string `yaml:"name"`
}

func (dp *DataPoint) toColumnDefinition() (*parquetschema.ColumnDefinition, error) {
	switch dp.Type {
	case "BOOLEAN":
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Type:           parquet.TypePtr(parquet.Type_BOOLEAN),
				Name:           dp.Name,
				RepetitionType: repetitionType(dp.Optional),
			},
		}, nil
	case "INT64":
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Type:           parquet.TypePtr(parquet.Type_INT64),
				Name:           dp.Name,
				RepetitionType: repetitionType(dp.Optional),
				LogicalType: &parquet.LogicalType{
					INTEGER: &parquet.IntType{
						BitWidth: 64,
						IsSigned: true,
					},
				},
			},
		}, nil
	case "INT32":
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Type:           parquet.TypePtr(parquet.Type_INT32),
				Name:           dp.Name,
				RepetitionType: repetitionType(dp.Optional),
				LogicalType: &parquet.LogicalType{
					INTEGER: &parquet.IntType{
						BitWidth: 32,
						IsSigned: true,
					},
				},
			},
		}, nil
	case "INT":
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Type:           parquet.TypePtr(parquet.Type_INT32),
				Name:           dp.Name,
				RepetitionType: repetitionType(dp.Optional),
				LogicalType: &parquet.LogicalType{
					INTEGER: &parquet.IntType{
						BitWidth: 32,
						IsSigned: true,
					},
				},
			},
		}, nil
	// float32
	case "FLOAT":
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Type:           parquet.TypePtr(parquet.Type_FLOAT),
				Name:           dp.Name,
				RepetitionType: repetitionType(dp.Optional),
			},
		}, nil
	// float64
	case "DOUBLE":
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Type:           parquet.TypePtr(parquet.Type_DOUBLE),
				Name:           dp.Name,
				RepetitionType: repetitionType(dp.Optional),
			},
		}, nil
	case "UUID":
		// use STRING for now since downstream consumers like BQ don't support UUID
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Type:           parquet.TypePtr(parquet.Type_BYTE_ARRAY),
				Name:           dp.Name,
				RepetitionType: repetitionType(dp.Optional),
				ConvertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
				LogicalType: &parquet.LogicalType{
					STRING: &parquet.StringType{},
				},
			},
		}, nil
	case "STRING":
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Type:           parquet.TypePtr(parquet.Type_BYTE_ARRAY),
				Name:           dp.Name,
				RepetitionType: repetitionType(dp.Optional),
				ConvertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_UTF8),
				LogicalType: &parquet.LogicalType{
					STRING: &parquet.StringType{},
				},
			},
		}, nil
	case "TIMESTAMP":
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Type:           parquet.TypePtr(parquet.Type_INT64),
				Name:           dp.Name,
				RepetitionType: repetitionType(dp.Optional),
				ConvertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_TIMESTAMP_MILLIS),
				LogicalType: &parquet.LogicalType{
					TIMESTAMP: &parquet.TimestampType{
						IsAdjustedToUTC: true,
						Unit: &parquet.TimeUnit{
							MILLIS: parquet.NewMilliSeconds(),
						},
					},
				},
			},
		}, nil
	case "DATE":
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Type:           parquet.TypePtr(parquet.Type_INT32),
				Name:           dp.Name,
				RepetitionType: repetitionType(dp.Optional),
				ConvertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_DATE),
				LogicalType: &parquet.LogicalType{
					DATE: &parquet.DateType{},
				},
			},
		}, nil
	case "DECIMAL":
		precision := int32(18)
		scale := int32(2)
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Type:           parquet.TypePtr(parquet.Type_INT64),
				Name:           dp.Name,
				RepetitionType: repetitionType(dp.Optional),
				ConvertedType:  parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL),
				LogicalType: &parquet.LogicalType{
					DECIMAL: &parquet.DecimalType{
						Precision: precision,
						Scale:     scale,
					},
				},
				// this is for compatibility with the deprecated ConvertedType
				// BQ does not currently support LogicalType
				Precision: &precision,
				Scale:     &scale,
			},
		}, nil
	case "BYTE_ARRAY":
		return &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Type:           parquet.TypePtr(parquet.Type_BYTE_ARRAY),
				Name:           dp.Name,
				RepetitionType: repetitionType(dp.Optional),
			},
		}, nil
	default:
		return nil, fmt.Errorf("unknown kind %s is unsupported", dp.Type)
	}

}

func repetitionType(optional bool) *parquet.FieldRepetitionType {
	if optional {
		return parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL)
	}
	return parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED)
}

func New(baseDir string) Catalog {
	return Catalog{baseDir}
}

func (c *Catalog) GetByID(id string) (*Definition, error) {
	// cheap enough to load every time considering we're only processing fairly
	// large parquet files for now
	defs, err := load(c.baseDir)
	if err != nil {
		return nil, err
	}
	if d, ok := defs[id]; ok {
		return &d, nil

	}
	return nil, ErrMissingDefinition
}

func load(baseDir string) (map[string]Definition, error) {
	defs := make(map[string]Definition)
	return defs, filepath.WalkDir(baseDir, func(path string, d fs.DirEntry, err error) error {

		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".yaml") {
			//log skipped
			return nil
		}

		var dp Definition
		b, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("failed to read %v err=%w", path, err)
		}

		err = yaml.Unmarshal(b, &dp)
		if err != nil {
			return fmt.Errorf("failed to unmarshal %v err=%w", path, err)
		}
		defs[dp.DataProduct.ID] = dp
		return nil
	})
}

func ToParquetSchema(def Definition) (*parquetschema.SchemaDefinition, error) {

	var dataPoints []*parquetschema.ColumnDefinition
	for _, dp := range def.DataProduct.DataPoints {
		cd, err := dp.toColumnDefinition()
		if err != nil {
			return nil, err
		}
		dataPoints = append(dataPoints, cd)
	}

	return &parquetschema.SchemaDefinition{
		RootColumn: &parquetschema.ColumnDefinition{
			SchemaElement: &parquet.SchemaElement{
				Name: def.DataProduct.ID,
			},
			Children: dataPoints,
		},
	}, nil
}
