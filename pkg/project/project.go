package project

var (
	version = "local-dev"
	commit  = "dirty"
)

func Version() string {
	return version
}

func Commit() string {
	return commit
}

func FullName() string {
	return "terraform-provider-clickhousedbops"
}
