package astipatch

import "flag"

// Flags
var (
	PatchesDirectoryPath = flag.String("astipatch-patches-directory-path", "", "the patches directory path")
)

// Configuration represents the patcher configuration
type Configuration struct {
	PatchesDirectoryPath string `toml:"patches_directory_path"`
}

// FlagConfig generates a Configuration based on flags
func FlagConfig() Configuration {
	return Configuration{
		PatchesDirectoryPath: *PatchesDirectoryPath,
	}
}
