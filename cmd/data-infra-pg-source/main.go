package main

import (
	"context"
	"fmt"
	"os"
	"time"

	_ "github.com/benthosdev/benthos/v4/public/components/all"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"github.com/utilitywarehouse/data-infra-pg-source/internal/benthos/parquet"
	"github.com/utilitywarehouse/data-infra-pg-source/internal/benthos/sql"
	"github.com/utilitywarehouse/data-infra-pg-source/internal/benthos/terminate"
	"github.com/utilitywarehouse/data-products-definitions/pkg/catalog/v1"
)

const appName = "data-infra-pg-source"

func main() {
	app := &cli.App{
		Name:   appName,
		Before: beforeFunc,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "log-level",
				Value:   "info",
				EnvVars: []string{"LOG_LEVEL"},
			},
			&cli.StringFlag{
				Name:    "catalog-dir",
				Value:   "/defs/data-products-definitions/dev",
				EnvVars: []string{"CATALOG_DIR"},
			},
			&cli.StringFlag{
				Name:    "data-product-id",
				EnvVars: []string{"DATA_PRODUCT_ID"},
			},
			&cli.StringFlag{
				Name:    "interval",
				Usage:   "@every 1h",
				EnvVars: []string{"INTERVAL"},
			},
			&cli.StringFlag{
				Name:    "dsn",
				Usage:   "postgres://postgres:admin@localhost:5432/postgres?sslmode=disable",
				EnvVars: []string{"DSN"},
			},
			&cli.StringFlag{
				Name:    "driver",
				Usage:   "postgres",
				EnvVars: []string{"DRIVER"},
			},
			&cli.StringFlag{
				Name:    "query",
				EnvVars: []string{"QUERY"},
			},
			&cli.StringFlag{
				Name:    "gs-bucket",
				EnvVars: []string{"GS_BUCKET"},
			},
			&cli.StringFlag{
				Name:    "gs-creds",
				EnvVars: []string{"GOOGLE_APPLICATION_CREDENTIALS"},
			},
			&cli.StringFlag{
				Name:    "config",
				Aliases: []string{"c"},
				Usage:   "a path to a configuration file",
			},
		},
		Action: func(c *cli.Context) error {
			if err := sql.New(); err != nil {
				return err
			}
			if err := terminate.New(); err != nil {
				return err
			}

			os.Setenv("CREATED_AT", fmt.Sprintf("%v", time.Now().Unix()))
			cat := catalog.New(c.String("catalog-dir"))
			if err := parquet.New(cat); err != nil {
				return err
			}
			service.RunCLI(context.Background())
			return nil
		},
	}
	if err := app.Run(os.Args); err != nil {
		logrus.WithError(err).Panic("error running application")
	}
}

func beforeFunc(c *cli.Context) error {
	logLevel, err := logrus.ParseLevel(c.String("log-level"))
	if err != nil {
		return err
	}
	logrus.SetLevel(logLevel)
	return nil
}
