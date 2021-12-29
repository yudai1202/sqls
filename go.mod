module github.com/lighttiger2505/sqls

go 1.16

require (
	github.com/aws/aws-sdk-go v1.40.58 // indirect
	github.com/denisenkom/go-mssqldb v0.11.0
	github.com/go-sql-driver/mysql v1.6.0
	github.com/google/go-cmp v0.5.6
	github.com/jackc/pgx/v4 v4.12.0
	github.com/k0kubun/colorstring v0.0.0-20150214042306-9440f1994b88 // indirect
	github.com/k0kubun/pp v2.4.0+incompatible
	github.com/mattn/go-sqlite3 v1.14.8
	github.com/olekukonko/tablewriter v0.0.5
	github.com/sourcegraph/jsonrpc2 v0.1.0
	github.com/uber/athenadriver v1.1.13
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519
	gopkg.in/yaml.v2 v2.4.0
)

replace github.com/lighttiger2505/sqls => ./
