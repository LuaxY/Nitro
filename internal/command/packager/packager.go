package packager

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"trancode/internal/command/root"
	"trancode/internal/executor"
	"trancode/internal/queue"
	"trancode/internal/storage"
	"trancode/internal/util"
)

func init() {
	root.Cmd.AddCommand(cmd)
}

var cmd = &cobra.Command{
	Use: "packager",
	Run: func(cmd *cobra.Command, args []string) {
		Run()
	},
}

func Run() {
	bucket, err := storage.NewLocal(context.Background(), root.Cmd.Flag("storage").Value.String())

	if err != nil {
		log.Fatal(err)
	}

	channel, err := queue.NewRabbitMQ(root.Cmd.Flag("amqp").Value.String())

	if err != nil {
		log.Fatal(err)
	}

	for {
		var req queue.PackagerRequest
		ok, err := channel.Consume("packer.request", &req)

		if err != nil {
			log.Fatal(err)
		}

		if !ok {
			time.Sleep(5 * time.Second)
			continue
		}

		_, err = pack(req.UID, bucket, req.Qualities)

		if err = channel.Publish("packer.response", queue.PackagerResponse{
			UID: req.UID,
		}); err != nil {
			log.Fatal(err)
		}
	}
}

type result struct {
	Path string
}

func pack(uid string, bucket storage.Bucket, qualities []int) (*result, error) {
	workDir, err := ioutil.TempDir(os.TempDir(), "merge")

	if err != nil {
		return nil, errors.Wrap(err, "unable to create temporary working directory")
	}

	if err = util.Download(bucket, uid+"/audio.m4a", workDir+"/audio.m4a"); err != nil {
		return nil, errors.Wrap(err, "unable to get audio file")
	}

	files := make(map[string]string)
	exec := executor.NewExecutor(&bytes.Buffer{})
	packager := &executor.Cmd{Binary: "packager"}

	for _, quality := range qualities {
		if err = util.Download(bucket, fmt.Sprintf("%s/%d.mp4", uid, quality), fmt.Sprintf("%s/%d.mp4", workDir, quality)); err != nil {
			return nil, errors.Wrap(err, "unable to get video file")
		}

		packager.Add(fmt.Sprintf("in=%[1]s/%[2]d.mp4,stream=video,output=%[1]s/out/%[2]d.mp4,playlist_name=%[1]s/out/%[2]d.m3u8,iframe_playlist_name=%[1]s/out/%[2]d_iframe.m3u8", workDir, quality))

		files[fmt.Sprintf("%s/%d.mp4", uid, quality)] = fmt.Sprintf("%s/out/%d.mp4", workDir, quality)
		files[fmt.Sprintf("%s/%d.m3u8", uid, quality)] = fmt.Sprintf("%s/out/%d.m3u8", workDir, quality)
		files[fmt.Sprintf("%s/%d_iframe.m3u8", uid, quality)] = fmt.Sprintf("%s/out/%d_iframe.m3u8", workDir, quality)
	}

	packager.Add(fmt.Sprintf("in=%[1]s/audio.m4a,stream=audio,output=%[1]s/out/audio.m4a,playlist_name=%[1]s/out/audio.m3u8,hls_group_id=audio", workDir))
	packager.Add("--hls_master_playlist_output", fmt.Sprintf("%s/out/master.m3u8", workDir))
	packager.Add("--mpd_output", fmt.Sprintf("%s/out/manifest.mpd", workDir))

	if err := exec.Run(packager); err != nil {
		return nil, errors.Wrap(err, "error while executing packager")
	}

	files[uid+"/audio.m4a"] = workDir + "/out/audio.m4a"
	files[uid+"/audio.m3u8"] = workDir + "/out/audio.m3u8"
	files[uid+"/master.m3u8"] = workDir + "/out/master.m3u8"
	files[uid+"/manifest.mpd"] = workDir + "/out/manifest.mpd"

	for key, file := range files {
		//log.Printf("upload '%s' to '%s'", file, key)

		if err = util.Upload(bucket, key, file); err != nil {
			return nil, errors.Wrapf(err, "unable to upload %s file", file)
		}
	}

	return &result{}, nil
}
