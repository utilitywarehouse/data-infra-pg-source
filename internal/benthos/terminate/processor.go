package terminate

import (
	"context"
	"os"

	"github.com/benthosdev/benthos/v4/public/service"
)

func New() error {
	configSpec := service.NewConfigSpec().
		Summary("Component for terminating pipelines in case of errors")

	if err := service.RegisterBatchProcessor("uw_terminate", configSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return &terminateProcessor{logger: mgr.Logger()}, nil
		},
	); err != nil {
		return err
	}

	return nil
}

type terminateProcessor struct {
	logger *service.Logger
}

func (t *terminateProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	t.logger.Error("Error encountered, terminating pipeline")
	os.Exit(1)
	return nil, nil
}

func (t *terminateProcessor) Close(ctx context.Context) error {
	return nil
}
