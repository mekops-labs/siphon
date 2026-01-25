package iotplotter

import (
	"bytes"
	"fmt"
	"net/http"

	"github.com/mekops-labs/siphon/pkg/sink"
	"github.com/mitchellh/mapstructure"
)

const baseUrl = "http://iotplotter.com/api/v2/feed"

type IotPlotterParams struct {
	Apikey string
	Feed   string
}

type iotplotter struct {
	apikey string
	feed   string
}

var _ sink.Sink = (*iotplotter)(nil)

func init() {
	sink.Registry.Add("iotplotter", New)
}

func New(p any) (sink.Sink, error) {

	var opt IotPlotterParams

	if err := mapstructure.Decode(p, &opt); err != nil {
		return nil, err
	}

	if opt.Apikey == "" || opt.Feed == "" {
		return nil, fmt.Errorf("iotplotter sink: apikey and feed are required fields")
	}

	return &iotplotter{
		apikey: opt.Apikey,
		feed:   opt.Feed,
	}, nil
}

func (p *iotplotter) Send(b []byte) error {
	r, err := http.NewRequest("POST", fmt.Sprintf("%s/%s", baseUrl, p.feed), bytes.NewBuffer(b))
	if err != nil {
		return err
	}

	r.Header.Add("Content-Type", "application/json")
	r.Header.Add("api-key", p.apikey)

	client := &http.Client{}
	_, err = client.Do(r)

	return err
}
