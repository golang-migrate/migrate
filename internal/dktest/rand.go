package dktest

import "math/rand/v2"

const (
	chars               = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	containerNamePrefix = "dktest_"
)

func randString(n uint) string {
	if n == 0 {
		return ""
	}
	b := make([]byte, n)
	for i := range b {
		b[i] = chars[rand.IntN(len(chars))]
	}
	return string(b)
}

func genContainerName() string {
	return containerNamePrefix + randString(10)
}
