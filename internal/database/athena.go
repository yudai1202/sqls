package database

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/lighttiger2505/sqls/dialect"
	drv "github.com/uber/athenadriver/go"
)

func init() {
	RegisterOpen(drv.DriverName, athenaOpen)
	RegisterFactory(drv.DriverName, NewAthenaDBRepository)
}

func athenaOpen(dbConnCfg *DBConfig) (*DBConnection, error) {
	var (
		conn *sql.DB
	)

	dsn, err := genAthenaConfig(dbConnCfg)
	if err != nil {
		return nil, err
	}

	if dbConnCfg.SSHCfg != nil {
		return nil, fmt.Errorf("ssh connection is not supported")
	} else {
		dbConn, err := sql.Open(drv.DriverName, dsn)
		if err != nil {
			return nil, err
		}
		conn = dbConn
	}
	if err = conn.Ping(); err != nil {
		return nil, err
	}

	conn.SetMaxIdleConns(DefaultMaxIdleConns)
	conn.SetMaxOpenConns(DefaultMaxOpenConns)

	return &DBConnection{
		Conn: conn,
	}, nil
}

type AthenaDBRepository struct {
	Conn *sql.DB
}

func NewAthenaDBRepository(conn *sql.DB) DBRepository {
	return &AthenaDBRepository{Conn: conn}
}

func (db *AthenaDBRepository) Driver() dialect.DatabaseDriver {
	return drv.DriverName
}

func (db *AthenaDBRepository) CurrentDatabase(ctx context.Context) (string, error) {
	return "", nil
}

func (db *AthenaDBRepository) Databases(ctx context.Context) ([]string, error) {
	rows, err := db.Conn.QueryContext(ctx, "SELECT schema_name FROM information_schema.schemata")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	databases := []string{}
	for rows.Next() {
		var database string
		if err := rows.Scan(&database); err != nil {
			return nil, err
		}
		databases = append(databases, database)
	}
	return databases, nil
}

func (db *AthenaDBRepository) CurrentSchema(ctx context.Context) (string, error) {
	return db.CurrentDatabase(ctx)
}

func (db *AthenaDBRepository) Schemas(ctx context.Context) ([]string, error) {
	return db.Databases(ctx)
}

func (db *AthenaDBRepository) SchemaTables(ctx context.Context) (map[string][]string, error) {
	// rows, err := db.Conn.QueryContext(
	// 	ctx,
	// 	`
	// SELECT
	// 	table_schema,
	// 	table_name
	// FROM
	// 	information_schema.tables
	// ORDER BY
	// 	table_schema,
	// 	table_name
	// `)
	// if err != nil {
	// 	return nil, err
	// }
	// defer rows.Close()
	databaseTables := map[string][]string{}
	// for rows.Next() {
	// 	var schema, table string
	// 	if err := rows.Scan(&schema, &table); err != nil {
	// 		return nil, err
	// 	}

	// 	if arr, ok := databaseTables[schema]; ok {
	// 		databaseTables[schema] = append(arr, table)
	// 	} else {
	// 		databaseTables[schema] = []string{table}
	// 	}
	// }
	return databaseTables, nil
}

func (db *AthenaDBRepository) Tables(ctx context.Context) ([]string, error) {
	// rows, err := db.Conn.QueryContext(
	// 	ctx,
	// 	`
	// SELECT
	//   table_name
	// FROM
	//   information_schema.tables
	// WHERE
	//   table_type = 'BASE TABLE'
	//   AND table_schema NOT IN ('pg_catalog', 'information_schema')
	// ORDER BY
	//   table_name
	// `)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer rows.Close()
	tables := []string{}
	// for rows.Next() {
	// 	var table string
	// 	if err := rows.Scan(&table); err != nil {
	// 		return nil, err
	// 	}
	// 	tables = append(tables, table)
	// }
	return tables, nil
}

func (db *AthenaDBRepository) DescribeDatabaseTable(ctx context.Context) ([]*ColumnDesc, error) {
	// rows, err := db.Conn.QueryContext(
	// 	ctx,
	// 	`
	// SELECT
	// 	c.table_schema,
	// 	c.table_name,
	// 	c.column_name,
	// 	c.data_type,
	// 	c.is_nullable,
	// 	CASE tc.constraint_type
	// 		WHEN 'PRIMARY KEY' THEN 'YES'
	// 		ELSE 'NO'
	// 	END,
	// 	c.column_default,
	// 	''
	// FROM
	// 	information_schema.columns c
	// LEFT JOIN
	// 	information_schema.constraint_column_usage ccu
	// 	ON c.table_name = ccu.table_name
	// 	AND c.column_name = ccu.column_name
	// LEFT JOIN information_schema.table_constraints tc ON
	// 	tc.table_catalog = c.table_catalog
	// 	AND tc.table_schema = c.table_schema
	// 	AND tc.table_name = c.table_name
	// 	AND tc.constraint_name = ccu.constraint_name
	// ORDER BY
	// 	c.table_name,
	// 	c.ordinal_position
	// `)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer rows.Close()
	tableInfos := []*ColumnDesc{}
	// for rows.Next() {
	// 	var tableInfo ColumnDesc
	// 	err := rows.Scan(
	// 		&tableInfo.Schema,
	// 		&tableInfo.Table,
	// 		&tableInfo.Name,
	// 		&tableInfo.Type,
	// 		&tableInfo.Null,
	// 		&tableInfo.Key,
	// 		&tableInfo.Default,
	// 		&tableInfo.Extra,
	// 	)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	tableInfos = append(tableInfos, &tableInfo)
	// }
	return tableInfos, nil
}

func (db *AthenaDBRepository) DescribeDatabaseTableBySchema(ctx context.Context, schemaName string) ([]*ColumnDesc, error) {
	// rows, err := db.Conn.QueryContext(
	// 	ctx,
	// 	`
	// SELECT
	// 	c.table_schema,
	// 	c.table_name,
	// 	c.column_name,
	// 	c.data_type,
	// 	c.is_nullable,
	// 	CASE tc.constraint_type
	// 		WHEN 'PRIMARY KEY' THEN 'YES'
	// 		ELSE 'NO'
	// 	END,
	// 	c.column_default,
	// 	''
	// FROM
	// 	information_schema.columns c
	// LEFT JOIN
	// 	information_schema.constraint_column_usage ccu
	// 	ON c.table_name = ccu.table_name
	// 	AND c.column_name = ccu.column_name
	// LEFT JOIN information_schema.table_constraints tc ON
	// 	tc.table_catalog = c.table_catalog
	// 	AND tc.table_schema = c.table_schema
	// 	AND tc.table_name = c.table_name
	// 	AND tc.constraint_name = ccu.constraint_name
	// WHERE
	// 	c.table_schema = $1
	// ORDER BY
	// 	c.table_name,
	// 	c.ordinal_position
	// `, schemaName)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer rows.Close()
	tableInfos := []*ColumnDesc{}
	// for rows.Next() {
	// 	var tableInfo ColumnDesc
	// 	err := rows.Scan(
	// 		&tableInfo.Schema,
	// 		&tableInfo.Table,
	// 		&tableInfo.Name,
	// 		&tableInfo.Type,
	// 		&tableInfo.Null,
	// 		&tableInfo.Key,
	// 		&tableInfo.Default,
	// 		&tableInfo.Extra,
	// 	)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	tableInfos = append(tableInfos, &tableInfo)
	// }
	return tableInfos, nil
}

func (db *AthenaDBRepository) Exec(ctx context.Context, query string) (sql.Result, error) {
	return db.Conn.ExecContext(ctx, query)
}

func (db *AthenaDBRepository) Query(ctx context.Context, query string) (*sql.Rows, error) {
	return db.Conn.QueryContext(ctx, query)
}

func genAthenaConfig(connCfg *DBConfig) (string, error) {
	if connCfg.DataSourceName != "" {
		return connCfg.DataSourceName, nil
	}
	os.Setenv("AWS_SDK_LOAD_CONFIG", "1")
	cfg, err := drv.NewDefaultConfig(connCfg.Params["OutputBucket"],
		connCfg.Params["Region"], "DummyAccessID", "DummySecretAccessKey")
	if err != nil {
		return "", err
	}
	cfg.SetAWSProfile(connCfg.Params["Profile"])
	// Specify the workgroup
	wg := drv.NewWG(connCfg.Params["WorkGroup"], nil, nil)
	cfg.SetWorkGroup(wg)
	cfg.SetWGRemoteCreationAllowed(false)

	return cfg.Stringify(), nil
}
