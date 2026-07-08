package dktest

import "errors"

var (
	errNoNetworkSettings = errors.New("no network settings")
	errNoPort            = errors.New("no port")
)
