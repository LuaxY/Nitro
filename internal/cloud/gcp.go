package cloud

import (
	"context"
	"path"

	"github.com/pkg/errors"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/compute/v1"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/logging/v2"
	"google.golang.org/api/option"
	"gopkg.in/yaml.v2"

	"nitro/internal/util"
)

type gcp struct {
	computeService *compute.Service
	project        string
	zone           string
	group          string
}

func NewGCP(ctx context.Context, project, zone, group string) (Provider, error) {
	httpClient, err := google.DefaultClient(ctx, compute.CloudPlatformScope)

	if err != nil {
		return nil, errors.Wrap(err, "gcp http client")
	}

	creds, err := google.FindDefaultCredentials(ctx)

	if err != nil {
		return nil, errors.Wrap(err, "gcp credentials")
	}

	computeService, err := compute.NewService(ctx, option.WithHTTPClient(httpClient), option.WithCredentials(creds))

	if err != nil {
		return nil, errors.Wrap(err, "gcp compute service")
	}

	return &gcp{computeService: computeService, project: project, zone: zone, group: group}, nil
}

func (g *gcp) Count(ctx context.Context) (int, error) {
	instances, err := g.Instances(ctx)

	if err != nil {
		return 0, errors.Wrap(err, "gcp group instances count")
	}

	count := 0

	for _, instance := range instances {
		switch instance.Status {
		case "PROVISIONING", "STAGING", "RUNNING", "REPAIRING":
			count++
		}
	}

	return count, nil
}

func (g *gcp) Instances(ctx context.Context) ([]*Instance, error) {
	resp, err := g.computeService.InstanceGroups.ListInstances(g.project, g.zone, g.group, nil).Context(ctx).Do()

	if err != nil {
		return nil, errors.Wrap(err, "gcp group instances list")
	}

	if resp.HTTPStatusCode != 200 {
		return nil, errors.Errorf("gcp group instances list bad http status code: %d", resp.HTTPStatusCode)
	}

	instances := make([]*Instance, len(resp.Items))

	for i, instance := range resp.Items {
		instances[i] = &Instance{
			Name:   instance.Instance,
			Status: instance.Status,
		}
	}

	return instances, nil
}

type ContainerDeclaration struct {
	Spec Spec `yaml:"spec"`
}

type Spec struct {
	Containers    []Container `yaml:"containers"`
	RestartPolicy string      `yaml:"restartPolicy"`
}

type Container struct {
	Name  string   `yaml:"name"`
	Image string   `yaml:"image"`
	Args  []string `yaml:"args"`
	Env   []Env    `yaml:"env"`
	Stdin bool     `yaml:"stdin"`
	TTY   bool     `yaml:"tty"`
}

type Env struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
}

var logDriver = `#cloud-config

write_files:
  - path: /etc/docker/daemon.json
    content: '{"log-driver":"gcplogs"}'

runcmd:
  - systemctl start stackdriver-logging
  - systemctl restart docker`

func (g *gcp) AddInstance(ctx context.Context, namePrefix string, machineType string, image string, preemptible bool) (string, error) {
	//prefix := "https://www.googleapis.com/compute/v1/projects/" + g.project
	prefix := "projects/" + g.project
	instanceName := namePrefix + util.Random(4)

	container := ContainerDeclaration{
		Spec: Spec{
			Containers: []Container{
				{
					Name:  instanceName,
					Image: "gcr.io/spheric-backup-272401/nitro:latest", // TODO var
					Args: []string{
						"encoder",
					},
					Env: []Env{
						{Name: "RABBITMQ_PROTOCOL", Value: "amqp"},
						{Name: "RABBITMQ_HOST", Value: "rabbitmq.nitro.voidmx.net"},
						{Name: "RABBITMQ_PORT", Value: "5672"},
						{Name: "RABBITMQ_USER", Value: "admin"},
						{Name: "RABBITMQ_PASS", Value: "GFLHmtFr7U9SNXhn"},
						{Name: "GCS_BUCKET", Value: "nitro-encoder"},
						{Name: "PROVIDER", Value: "google-vm"},
					},
					Stdin: false,
					TTY:   false,
				},
			},
			//RestartPolicy: "Never",
			RestartPolicy: "Always",
		},
	}

	containerYaml, _ := yaml.Marshal(container)

	image = "projects/cos-cloud/global/images/cos-stable-81-12871-119-0" // TODO

	instance := &compute.Instance{
		Name:        instanceName,
		MachineType: prefix + "/zones/" + g.zone + "/machineTypes/" + machineType,
		Disks: []*compute.AttachedDisk{
			{
				AutoDelete: true,
				Boot:       true,
				Type:       "PERSISTENT",
				DeviceName: instanceName,
				InitializeParams: &compute.AttachedDiskInitializeParams{
					//DiskName: instanceName,
					//SourceImage: prefix + "/global/images/" + image, // TODO restore
					SourceImage: image,
					DiskSizeGb:  10, // TODO remove ?
				},
			},
		},
		NetworkInterfaces: []*compute.NetworkInterface{
			{
				Network: prefix + "/global/networks/default",
				AccessConfigs: []*compute.AccessConfig{
					{
						NetworkTier: "STANDARD",
					},
				},
			},
		},
		ServiceAccounts: []*compute.ServiceAccount{
			{
				Email: "default",
				Scopes: []string{
					compute.ComputeScope,
					compute.DevstorageFullControlScope,
					logging.LoggingWriteScope,
				},
			},
		},
		Metadata: &compute.Metadata{
			Items: []*compute.MetadataItems{
				/*{
					Key:   "startup-script-url",
					Value: googleapi.String("gs://nitro-encoder/startup-script.sh"),
					//Value: googleapi.String("https://nitro.s3.fr-par.scw.cloud/startup-script.sh"),
				},
				{
					Key:   "shutdown-script-url",
					Value: googleapi.String("gs://nitro-encoder/shutdown-script.sh"),
					//Value: googleapi.String("https://nitro.s3.fr-par.scw.cloud/shutdown-script.sh"),
				},*/
				{
					Key:   "gce-container-declaration",
					Value: googleapi.String(string(containerYaml)),
				},
				{
					Key:   "user-data",
					Value: googleapi.String(logDriver),
				},
			},
		},
		Tags: &compute.Tags{
			Items: []string{"nitro", "video", "encoder", "ffmpeg"},
		},
		Scheduling: &compute.Scheduling{
			AutomaticRestart: googleapi.Bool(false),
			Preemptible:      preemptible,
		},
	}

	resp, err := g.computeService.Instances.Insert(g.project, g.zone, instance).Context(ctx).Do()

	if err != nil {
		return "", errors.Wrap(err, "gcp add instance")
	}

	if resp.HTTPStatusCode != 200 {
		return "", errors.Errorf("gcp add instance bad http status code: %d", resp.HTTPStatusCode)
	}

	resp, err = g.computeService.InstanceGroups.AddInstances(g.project, g.zone, g.group, &compute.InstanceGroupsAddInstancesRequest{
		Instances: []*compute.InstanceReference{
			{
				Instance: "https://www.googleapis.com/compute/v1/projects/" + g.project + "/zones/" + g.zone + "/instances/" + instance.Name,
			},
		},
	}).Context(ctx).Do()

	if err != nil {
		return "", errors.Wrap(err, "gcp add instance to group")
	}

	if resp.HTTPStatusCode != 200 {
		return "", errors.Errorf("gcp add instance to group bad http status code: %d", resp.HTTPStatusCode)
	}

	return instance.Name, nil
}

func (g *gcp) DeleteInstance(ctx context.Context, instance string) error {
	resp, err := g.computeService.Instances.Delete(g.project, g.zone, instance).Context(ctx).Do()

	if err != nil {
		return errors.Wrap(err, "gcp delete instance")
	}

	if resp.HTTPStatusCode != 200 {
		return errors.Errorf("gcp delete instance bad http status code: %d", resp.HTTPStatusCode)
	}

	return nil
}

func (g *gcp) DeleteAll(ctx context.Context) error {
	resp, err := g.computeService.InstanceGroups.ListInstances(g.project, g.zone, g.group, nil).Context(ctx).Do()

	if err != nil {
		return errors.Wrap(err, "gcp list group instances")
	}

	if resp.HTTPStatusCode != 200 {
		return errors.Errorf("gcp list group instances bad http status code: %d", resp.HTTPStatusCode)
	}

	for _, instance := range resp.Items {
		if err = g.DeleteInstance(ctx, path.Base(instance.Instance)); err != nil {
			return err
		}
	}

	return nil
}
