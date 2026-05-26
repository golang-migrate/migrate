package dktest

import "math/rand/v2"

func genContainerName() string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 10)
	for i := range b {
		b[i] = letters[rand.IntN(len(letters))]
	}
	return "dktest-" + string(b)
}
