package migrate

import (
	"fmt"
	"io"
	"os"
	"strings"
	"text/template"
)

var envMapCache map[string]string

func envMap() map[string]string {
	if envMapCache != nil {
		return envMapCache
	}
	envMapCache = make(map[string]string)
	for _, kvp := range os.Environ() {
		kvParts := strings.SplitN(kvp, "=", 2)
		envMapCache[kvParts[0]] = kvParts[1]
	}
	return envMapCache
}

func applyEnvironmentTemplate(body io.ReadCloser) (io.ReadCloser, error) {
	bodyBytes, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("reading body: %w", err)
	}
	defer func() {
		_ = body.Close()
	}()

	tmpl, err := template.New("migration").Parse(string(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("parsing template: %w", err)
	}

	r, w := io.Pipe()

	go func() {
		_ = tmpl.Execute(w, envMap())
		_ = w.Close()
	}()

	return r, nil
}
