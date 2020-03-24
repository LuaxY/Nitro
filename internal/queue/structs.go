package queue

type SplitterRequest struct {
	UID       string   `yaml:"uid"`
	Input     string   `yaml:"input"`
	ChunkTime int      `yaml:"chunkTime"`
	Params    []Params `yaml:"params"`
}

type SplitterResponse struct {
	UID         string   `yaml:"uid"`
	TotalChunks int      `yaml:"totalChunks"`
	Chunks      []string `yaml:"chunks"`
	Params      []Params `yaml:"params"`
}

type EncoderRequest struct {
	UID    string   `yaml:"uid"`
	Chunk  string   `yaml:"chunk"`
	Params []Params `yaml:"params"`
}

type EncoderResponse struct {
	UID       string         `yaml:"uid"`
	ID        int            `yaml:"id"`
	Qualities map[int]string `yaml:"qualities"`
}

type MergerRequest struct {
	UID    string                 `yaml:"uid"`
	Chunks map[int]map[int]string `yaml:"chunks"`
}

type MergerResponse struct {
	UID       string `yaml:"uid"`
	Qualities []int  `yaml:"qualities"`
}

type PackagerRequest struct {
	UID       string `yaml:"uid"`
	Qualities []int  `yaml:"qualities"`
}

type PackagerResponse struct {
	UID       string   `yaml:"uid"`
	Manifests []string `yaml:"manifests"`
}

type Params struct {
	Map       int      `yaml:"map,omitempty"`
	Profile   string   `yaml:"profile,omitempty"`
	Codec     string   `yaml:"codec,omitempty"`
	Width     int      `yaml:"width,omitempty"`
	Height    int      `yaml:"height,omitempty"`
	CRF       int      `yaml:"crf,omitempty"`
	BitRate   string   `yaml:"bitrate,omitempty"`
	MaxRate   string   `yaml:"maxrate,omitempty"`
	MinRate   string   `yaml:"minrate,omitempty"`
	BufSize   string   `yaml:"bufsize,omitempty"`
	Preset    string   `yaml:"preset,omitempty"`
	ExtraArgs []string `yaml:"extraArgs,omitempty"`
}
