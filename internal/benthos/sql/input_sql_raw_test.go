package sql

import (
	"context"
	"testing"

	"github.com/benthosdev/benthos/v4/public/service"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSqlRawInput(t *testing.T) {
	conf := `
driver: postgres
dsn: postgres://admin:admin@localhost:5432/test?sslmode=disable
query: select * from example;
`

	spec := RawInputConfig()
	env := service.NewEnvironment()

	selectConfig, err := spec.ParseYAML(conf, env)
	require.NoError(t, err)

	sqlRawInput, err := newSQLRawInputFromConfig(selectConfig, nil)
	require.NoError(t, err)
	assert.Equal(t, "postgres", sqlRawInput.driver)
	assert.Equal(t, "postgres://admin:admin@localhost:5432/test?sslmode=disable", sqlRawInput.dsn)
	assert.Equal(t, "select * from example;", sqlRawInput.query)

	require.NoError(t, sqlRawInput.Close(context.Background()))
}
