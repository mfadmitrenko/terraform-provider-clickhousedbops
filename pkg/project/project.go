package project

var (
	version = "1.1.1"
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
