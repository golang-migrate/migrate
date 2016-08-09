package gomethods

import (
	"sync"
)

var methodsReceiversMu sync.Mutex
var methodsReceivers = make(map[string]interface{})

// Registers a methods receiver object so it can be created from its name. Users of gomethods migration drivers should
// call this method to register objects with their migration methods before executing the migration
func RegisterMethodsReceiver(name string, receiver interface{}) {
	methodsReceiversMu.Lock()
	defer methodsReceiversMu.Unlock()
	if receiver == nil {
		panic("Go methods: Register receiver object is nil")
	}
	if _, dup := methodsReceivers[name]; dup {
		panic("Go methods: Register called twice for receiver " + name)
	}
	methodsReceivers[name] = receiver
}

// Retrieves a registered driver by name
func GetMethodsReceiver(name string) interface{} {
	methodsReceiversMu.Lock()
	defer methodsReceiversMu.Unlock()
	receiver := methodsReceivers[name]
	return receiver
}
