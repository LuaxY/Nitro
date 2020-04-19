package cloud

import "context"

type Provider interface {
	Count(ctx context.Context) (int, error)
	Instances(ctx context.Context) ([]*Instance, error)
	AddInstance(ctx context.Context, namePrefix string, machineType string, image string, preemptible bool) (string, error)
	DeleteInstance(ctx context.Context, instance string) error
	DeleteAll(ctx context.Context) error
}

type Instance struct {
	Name   string
	Status string
}
