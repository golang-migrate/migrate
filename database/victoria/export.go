package victoria

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// Export reads data from VictoriaMetrics based on label filters and time range
func (v *Victoria) Export(ctx context.Context, w io.Writer) error {
	if !v.isOpen {
		return ErrClosed
	}

	// Build query parameters
	query := url.Values{}
	if v.config.LabelFilter != "" {
		query.Set("match[]", v.config.LabelFilter)
	}
	if v.config.StartTime != "" {
		query.Set("start", v.config.StartTime)
	}
	if v.config.EndTime != "" {
		query.Set("end", v.config.EndTime)
	}

	// Create request with context
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.exportURL+"?"+query.Encode(), nil)
	if err != nil {
		return fmt.Errorf("failed to create export request: %w", err)
	}

	// Set headers
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "gzip")

	// Execute request
	resp, err := v.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute export request: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("export failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	// Copy response body to writer
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read export data: %w", err)
	}

	return nil
}
