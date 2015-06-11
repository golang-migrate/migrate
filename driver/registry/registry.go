// Package registry maintains a map of imported and available drivers
package registry

var driverRegistry map[string]interface{}

// Registers a driver so it can be created from its name. Drivers should
// call this from an init() function so that they registers themselvse on
// import
func RegisterDriver(name string, driver interface{}) {
	driverRegistry[name] = driver
}

// Retrieves a registered driver by name
func GetDriver(name string) interface{} {
	return driverRegistry[name]
}

func init() {
	driverRegistry = make(map[string]interface{})
}
