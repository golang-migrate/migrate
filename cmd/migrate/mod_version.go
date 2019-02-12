// +build go1.12

package main

func init() {
	if info, available := debug.ReadBuildInfo(); available {
		if Version == "dev" {
			Version = info.Main.Version
		}
	}
}
