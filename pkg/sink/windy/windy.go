package windy

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/mekops-labs/siphon/pkg/bus"
	"github.com/mekops-labs/siphon/pkg/sink"
	"github.com/mitchellh/mapstructure"
)

var (
	windyProto = "https"
	windyHost  = "stations.windy.com"
	windyPath  = "/api/v2/observation/update"
)

type windyParams struct {
	Password string `mapstructure:"password"` // Station Password
	ID       any    `mapstructure:"id"`       // Station ID
}

type windySink struct {
	params windyParams
	client *http.Client
}

// Ensure windySink implements sink.Sink
var _ sink.Sink = (*windySink)(nil)

func init() {
	sink.Registry.Add("windy", New)
}

func New(params any, _bus bus.Bus) (sink.Sink, error) {
	var opt windyParams
	if err := mapstructure.Decode(params, &opt); err != nil {
		return nil, fmt.Errorf("failed to decode windy params: %w", err)
	}

	if opt.Password == "" {
		return nil, fmt.Errorf("windy requires a station password")
	}

	if opt.ID == nil {
		return nil, fmt.Errorf("windy requires a station id")
	}

	return &windySink{
		params: opt,
		client: &http.Client{Timeout: 10 * time.Second},
	}, nil
}

func (s *windySink) Send(b []byte) error {
	var payload map[string]interface{}
	if err := json.Unmarshal(b, &payload); err != nil {
		return fmt.Errorf("windy sink failed to parse incoming payload: %w", err)
	}

	query := url.Values{}

	query.Add("station", fmt.Sprintf("%v", s.params.ID))

	for key, val := range payload {
		query.Add(key, fmt.Sprintf("%v", val))
	}

	targetURL := url.URL{
		Scheme:   windyProto,
		Host:     windyHost,
		Path:     windyPath,
		RawQuery: query.Encode(),
	}

	req, err := http.NewRequest(http.MethodGet, targetURL.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+s.params.Password)

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("windy request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("windy returned non-2xx status: %d", resp.StatusCode)
	} else {
		log.Printf("Successfully sent data to Windy for station %s: %s", s.params.ID, targetURL.String())
	}

	return nil
}
