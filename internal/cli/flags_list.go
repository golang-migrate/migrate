package cli

type flagsList struct {
	Help        bool
	Version     bool
	Verbose     bool
	Prefetch    uint
	LockTimeout uint
	Path        string
	Database    string
	Source      string
}

var DefaultFlags = flagsList{
	Help:        false,
	Version:     false,
	Verbose:     false,
	Prefetch:    10,
	LockTimeout: 15,
	Path:        "",
	Database:    "",
	Source:      "",
}
