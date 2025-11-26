package v1

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "net/http"
    "strings"
    "sync"
    "time"
)

// SeaTableClient holds connection info for a base.
type SeaTableClient struct {
    Server   string
    BaseUUID string
    Token    string
}

var (
    seaTableClients   = make(map[string]*SeaTableClient)
    seaTableClientsMu sync.RWMutex
)

// registerSeaTableClient stores cfg and returns a clientId.
func registerSeaTableClient(cfg *SeaTableClient) string {
    seaTableClientsMu.Lock()
    defer seaTableClientsMu.Unlock()
    id := fmt.Sprintf("st-%s-%d", strings.ReplaceAll(cfg.BaseUUID, "-", ""), time.Now().UnixNano())
    seaTableClients[id] = cfg
    return id
}

func getSeaTableClient(id string) (*SeaTableClient, bool) {
    seaTableClientsMu.RLock()
    defer seaTableClientsMu.RUnlock()
    cfg, ok := seaTableClients[id]
    return cfg, ok
}

func trimTrailingSlash(s string) string {
    s = strings.TrimSpace(s)
    return strings.TrimRight(s, "/")
}

// doSeaTableRequest marshals body (if not nil) and performs an HTTP request.
func doSeaTableRequest(
    ctx context.Context,
    method string,
    url string,
    token string,
    body interface{},
) ([]byte, int, error) {
    var buf *bytes.Reader
    if body != nil {
        b, err := json.Marshal(body)
        if err != nil {
            return nil, 0, fmt.Errorf("marshal request body: %w", err)
        }
        buf = bytes.NewReader(b)
    } else {
        buf = bytes.NewReader([]byte{})
    }

    req, err := http.NewRequestWithContext(ctx, method, url, buf)
    if err != nil {
        return nil, 0, fmt.Errorf("create request: %w", err)
    }

    if strings.TrimSpace(token) != "" {
        req.Header.Set("Authorization", "Bearer "+token)
    }
    if body != nil {
        req.Header.Set("Content-Type", "application/json")
    }
    req.Header.Set("Accept", "application/json")

    client := &http.Client{Timeout: 30 * time.Second}
    resp, err := client.Do(req)
    if err != nil {
        return nil, 0, fmt.Errorf("http request failed: %w", err)
    }
    defer resp.Body.Close()

    buf2 := new(bytes.Buffer)
    if _, err := buf2.ReadFrom(resp.Body); err != nil {
        return nil, resp.StatusCode, fmt.Errorf("read response body: %w", err)
    }

    return buf2.Bytes(), resp.StatusCode, nil
}
