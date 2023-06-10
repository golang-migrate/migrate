package envvars

import (
	"os"
	"strconv"
)

func GetVarOrString(key, alternative string) string {
	result := os.Getenv(key)
	if result == "" {
		return alternative
	}
	return result
}

func GetVarOrInt(key string, alternative int) int {
	result := os.Getenv(key)
	if result == "" {

		return alternative
	}

	varInt, varIntErr := strconv.Atoi(result)
	if varIntErr != nil {
		return alternative
	}

	return varInt
}
