package transcode

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	"nitro/internal/util"
)

// Transcoder Main struct
type Transcoder struct {
	command       []string
	inputFile     string
	stdErrPipe    io.ReadCloser
	stdStdinPipe  io.WriteCloser
	process       *exec.Cmd
	inputDuration string
}

type Progress struct {
	FramesProcessed  string
	CurrentTime      string
	CurrentDuration  time.Duration
	CompleteDuration time.Duration
	CurrentBitrate   string
	Progress         float64
	Speed            string
}

type Metadata struct {
	Format Format `json:"format"`
}

type Format struct {
	Duration string `json:"duration"`
}

// Initialize Init the transcoding process
func (t *Transcoder) Initialize(inputPath string, command []string) error {
	var err error
	var outb, errb bytes.Buffer
	var metadata Metadata

	ffprobeCommand := []string{"-i", inputPath, "-print_format", "json", "-show_format", "-show_streams", "-show_error"}

	cmd := exec.Command("ffprobe", ffprobeCommand...)
	cmd.Stdout = &outb
	cmd.Stderr = &errb

	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("error executing (%s) | error: %s | message: %s %s", ffprobeCommand, err, outb.String(), errb.String())
	}

	if err = json.Unmarshal([]byte(outb.String()), &metadata); err != nil {
		return err
	}

	t.command = command
	t.inputFile = inputPath
	t.inputDuration = metadata.Format.Duration

	return nil
}

// Run Starts the transcoding process
func (t *Transcoder) Run(progress bool) <-chan error {
	done := make(chan error)
	command := t.command

	if !progress {
		command = append([]string{"-nostats", "-loglev mel", "0"}, command...)
	}

	command = append([]string{"-y", "-i", t.inputFile}, command...)

	fmt.Printf("ffmpeg %s\n", command)

	proc := exec.Command("ffmpeg", command...)

	if progress {
		errStream, err := proc.StderrPipe()

		if err != nil {
			fmt.Println("Progress not available (stdErr): " + err.Error())
		} else {
			t.stdErrPipe = errStream
		}
	}

	// Set the stdinPipe in case we need to stop the transcoding
	stdin, err := proc.StdinPipe()

	if nil != err {
		fmt.Println("Stdin not available: " + err.Error())
	}

	t.stdStdinPipe = stdin

	// If the user has requested progress, we send it to them on a Buffer
	var outb, errb bytes.Buffer

	if progress {
		proc.Stdout = &outb
		//proc.Stderr = &errb
	}

	err = proc.Start()

	go func(err error) {
		if err != nil {
			done <- fmt.Errorf("failed start FFMPEG with %s, message %s %s", err, outb.String(), errb.String())
			close(done)
			return
		}

		err = proc.Wait()

		if err != nil {
			err = fmt.Errorf("failed finish FFMPEG with %s message %s %s", err, outb.String(), errb.String())
		}
		done <- err
		close(done)
	}(err)

	return done
}

// Stop Ends the transcoding process
func (t *Transcoder) Stop() error {
	if t.process != nil {

		stdin := t.stdStdinPipe
		if stdin != nil {
			_, _ = stdin.Write([]byte("q\n"))
		}
	}
	return nil
}

// Output Returns the transcoding progress channel
func (t Transcoder) Output() <-chan Progress {
	out := make(chan Progress)

	go func() {
		defer close(out)
		if t.stdErrPipe == nil {
			out <- Progress{}
			return
		}

		defer func() {
			_ = t.stdErrPipe.Close()
		}()

		scanner := bufio.NewScanner(t.stdErrPipe)

		split := func(data []byte, atEOF bool) (advance int, token []byte, spliterror error) {
			if atEOF && len(data) == 0 {
				return 0, nil, nil
			}
			if i := bytes.IndexByte(data, '\n'); i >= 0 {
				// We have a full newline-terminated line.
				return i + 1, data[0:i], nil
			}
			if i := bytes.IndexByte(data, '\r'); i >= 0 {
				// We have a cr terminated line
				return i + 1, data[0:i], nil
			}
			if atEOF {
				return len(data), data, nil
			}

			return 0, nil, nil
		}

		scanner.Split(split)
		buf := make([]byte, 2)
		scanner.Buffer(buf, bufio.MaxScanTokenSize)

		for scanner.Scan() {
			progress := new(Progress)
			line := scanner.Text()
			if strings.Contains(line, "frame=") && strings.Contains(line, "time=") && strings.Contains(line, "bitrate=") {
				var re = regexp.MustCompile(`=\s+`)
				st := re.ReplaceAllString(line, `=`)

				f := strings.Fields(st)
				var framesProcessed string
				var currentTime string
				var currentBitrate string
				var currentSpeed string

				for j := 0; j < len(f); j++ {
					field := f[j]
					fieldSplit := strings.Split(field, "=")

					if len(fieldSplit) > 1 {
						fieldname := strings.Split(field, "=")[0]
						fieldvalue := strings.Split(field, "=")[1]

						if fieldname == "frame" {
							framesProcessed = fieldvalue
						}

						if fieldname == "time" {
							currentTime = fieldvalue
						}

						if fieldname == "bitrate" {
							currentBitrate = fieldvalue
						}
						if fieldname == "speed" {
							currentSpeed = fieldvalue
						}
					}
				}

				timesec := util.DurationToSec(currentTime)
				dursec, _ := strconv.ParseFloat(t.inputDuration, 64)
				//live stream check
				if dursec != 0 {
					// Progress calculation
					currentProgress := (timesec * 100) / dursec
					progress.Progress = currentProgress
				}
				progress.CurrentBitrate = currentBitrate
				progress.FramesProcessed = framesProcessed
				progress.CurrentTime = currentTime
				progress.CurrentDuration, _ = time.ParseDuration(fmt.Sprintf("%fs", timesec))
				progress.CompleteDuration, _ = time.ParseDuration(fmt.Sprintf("%fs", dursec))
				progress.Speed = currentSpeed
				out <- *progress
			}
		}
	}()

	return out
}
