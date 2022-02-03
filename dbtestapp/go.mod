module testapp

go 1.15

replace github.com/omec-project/MongoDBLibrary => ../../MongoDBLibrary

require (
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/json-iterator/go v1.1.11 // indirect
	github.com/omec-project/MongoDBLibrary v0.0.0-20220131215156-469b082e6122
	github.com/stretchr/testify v1.7.0 // indirect
	go.mongodb.org/mongo-driver v1.7.3
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c // indirect
	golang.org/x/sys v0.0.0-20210510120138-977fb7262007 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)
