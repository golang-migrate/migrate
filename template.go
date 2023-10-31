package migrate

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"text/template"
)

var envMapCache map[string]string
var envMapCacheLock sync.Mutex

func envMap() map[string]string {
	// get the lock before accessing envMap to prevent concurrent reads and writes
	envMapCacheLock.Lock()
	defer envMapCacheLock.Unlock()
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

func applyEnvironmentTemplate(body io.ReadCloser, logger Logger) (io.ReadCloser, error) {
	bodyBytes, err := io.ReadAll(body)
	if err != nil {
		return nil, fmt.Errorf("reading body: %w", err)
	}
	defer func() {
		err = body.Close()
		if err != nil {
			logger.Printf("applyEnvironmentTemplate: error closing body: %v", err)
		}
	}()

	tmpl, err := template.New("migration").Parse(string(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("parsing template: %w", err)
	}

	r, w := io.Pipe()

	go func() {
		em := envMap()
		err = tmpl.Execute(w, em)
		if err != nil {
			if logger != nil {
				logger.Printf("applyEnvironmentTemplate: error executing template: %v", err)
				if logger.Verbose() {
					logger.Printf("applyEnvironmentTemplate: env map used for template execution: %v", em)
				}
			}
		}
		err = w.Close()
		if err != nil {
			if logger != nil {
				logger.Printf("applyEnvironmentTemplate: error closing writer: %v", err)
			}
		}
	}()

	return r, nil
}
