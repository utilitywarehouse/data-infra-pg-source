package sql

import (
	"context"
	"database/sql"

	"github.com/benthosdev/benthos/v4/public/service"
)

func RawInputConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Categories("Integration").
		Summary("Executes a select query and creates a message for each row received.").
		Description(`Once the rows from the query are exhausted this input shuts down, allowing the pipeline to gracefully terminate (or the next input in a [sequence](/docs/components/inputs/sequence) to execute).`).
		Field(driverField).
		Field(dsnField).
		Field(rawQueryField().
			Example("SELECT * FROM footable WHERE user_id = $1;"))

	for _, f := range connFields() {
		spec = spec.Field(f)
	}

	spec = spec.Version("3.65.0").
		Example(
			"Table Query (PostgreSQL)",
			`Here we query a database for columns of footable that. A [branch processor](/docs/components/processors/branch) can be
used to batch rows before further processing`,
			`
input:
  uw_sql_raw:
    driver: postgres
    dsn: postgres://foouser:foopass@localhost:5432/testdb?sslmode=disable
    query: "SELECT * FROM footable;"
`,
		)
	return spec
}

func init() {
	err := service.RegisterInput(
		"uw_sql_raw", RawInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			i, err := newSQLRawInputFromConfig(conf, mgr.Logger())
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacks(i), nil
		})

	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type sqlRawInput struct {
	driver string
	dsn    string
	query  string
	db     *sql.DB
	rows   *sql.Rows

	connSettings connSettings

	logger *service.Logger
}

func newSQLRawInputFromConfig(conf *service.ParsedConfig, logger *service.Logger) (*sqlRawInput, error) {
	driverStr, err := conf.FieldString("driver")
	if err != nil {
		return nil, err
	}

	dsnStr, err := conf.FieldString("dsn")
	if err != nil {
		return nil, err
	}

	query, err := conf.FieldString("query")
	if err != nil {
		return nil, err
	}

	connSettings, err := connSettingsFromParsed(conf)
	if err != nil {
		return nil, err
	}

	return &sqlRawInput{
		driver:       driverStr,
		dsn:          dsnStr,
		query:        query,
		connSettings: connSettings,
		logger:       logger,
	}, nil
}

func (s *sqlRawInput) Connect(ctx context.Context) (err error) {
	if s.db != nil {
		return nil
	}

	var db *sql.DB
	if db, err = sqlOpenWithReworks(s.logger, s.driver, s.dsn); err != nil {
		return
	}
	s.connSettings.apply(db)

	var rows *sql.Rows
	if rows, err = db.QueryContext(context.Background(), s.query); err != nil {
		return
	}

	s.db = db
	s.rows = rows

	return nil
}

func (s *sqlRawInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	if s.db == nil && s.rows == nil {
		return nil, nil, service.ErrNotConnected
	}

	if s.rows == nil {
		return nil, nil, service.ErrEndOfInput
	}

	if !s.rows.Next() {
		err := s.rows.Err()
		if err == nil {
			err = service.ErrEndOfInput
		}
		_ = s.rows.Close()
		s.rows = nil
		return nil, nil, err
	}

	obj, err := sqlRowToMap(s.rows)
	if err != nil {
		_ = s.rows.Close()
		s.rows = nil
		return nil, nil, err
	}

	msg := service.NewMessage(nil)
	msg.SetStructured(obj)
	return msg, func(ctx context.Context, err error) error {
		// Nacks are handled by AutoRetryNacks because we don't have an explicit
		// ack mechanism right now.
		return nil
	}, nil
}

// Close the component, blocks until either the underlying resources are
// cleaned up or the context is cancelled. Returns an error if the context
// is cancelled.
func (s *sqlRawInput) Close(ctx context.Context) error {
	closeCtx, cancel := context.WithCancel(context.Background())
	go func() {
		if s.rows != nil {
			_ = s.rows.Close()
			s.rows = nil
		}
		if s.db != nil {
			_ = s.db.Close()
		}
		cancel()
	}()

	select {
	case <-closeCtx.Done():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func sqlRowToMap(rows *sql.Rows) (map[string]interface{}, error) {
	columnNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	values := make([]interface{}, len(columnNames))
	valuesWrapped := make([]interface{}, len(columnNames))
	for i := range values {
		valuesWrapped[i] = &values[i]
	}
	if err := rows.Scan(valuesWrapped...); err != nil {
		return nil, err
	}
	jObj := map[string]interface{}{}
	for i, v := range values {
		switch t := v.(type) {
		case string:
			jObj[columnNames[i]] = t
		case []byte:
			jObj[columnNames[i]] = string(t)
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			jObj[columnNames[i]] = t
		case float32, float64:
			jObj[columnNames[i]] = t
		case bool:
			jObj[columnNames[i]] = t
		default:
			jObj[columnNames[i]] = t
		}
	}
	return jObj, nil
}
