package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// DatabaseConfig holds connection configuration
type DatabaseConfig struct {
	Host     string
	Port     string
	Username string
	Password string
	Database string
}

// MigrationConfig holds migration settings
type MigrationConfig struct {
	Source      DatabaseConfig
	Destination DatabaseConfig
	BatchSize   int
	SkipTables  []string
	LogFile     string
}

// Logger handles logging to file and console
type Logger struct {
	file *os.File
}

func NewLogger(filename string) (*Logger, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &Logger{file: file}, nil
}

func (l *Logger) Log(message string) {
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	logMsg := fmt.Sprintf("[%s] %s\n", timestamp, message)
	fmt.Print(logMsg)
	if l.file != nil {
		l.file.WriteString(logMsg)
	}
}

func (l *Logger) Close() {
	if l.file != nil {
		l.file.Close()
	}
}

// ForeignKeyInfo represents a foreign key constraint
type ForeignKeyInfo struct {
	TableName        string
	ColumnName       string
	ReferencedTable  string
	ReferencedColumn string
}

// DatabaseMigrator handles the migration process
type DatabaseMigrator struct {
	sourceDB *sql.DB
	destDB   *sql.DB
	config   MigrationConfig
	logger   *Logger
}

func NewDatabaseMigrator(config MigrationConfig) (*DatabaseMigrator, error) {
	logger, err := NewLogger(config.LogFile)
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %v", err)
	}

	migrator := &DatabaseMigrator{
		config: config,
		logger: logger,
	}

	// Connect to source database
	sourceDSN := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		config.Source.Username, config.Source.Password,
		config.Source.Host, config.Source.Port, config.Source.Database)

	migrator.sourceDB, err = sql.Open("mysql", sourceDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to source database: %v", err)
	}

	// Connect to destination database
	destDSN := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		config.Destination.Username, config.Destination.Password,
		config.Destination.Host, config.Destination.Port, config.Destination.Database)

	migrator.destDB, err = sql.Open("mysql", destDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to destination database: %v", err)
	}

	// Test connections
	if err := migrator.sourceDB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping source database: %v", err)
	}

	if err := migrator.destDB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping destination database: %v", err)
	}

	logger.Log("Successfully connected to both databases")
	return migrator, nil
}

func (dm *DatabaseMigrator) Close() {
	if dm.sourceDB != nil {
		dm.sourceDB.Close()
	}
	if dm.destDB != nil {
		dm.destDB.Close()
	}
	if dm.logger != nil {
		dm.logger.Close()
	}
}

// GetTables retrieves all table names from source database
func (dm *DatabaseMigrator) GetTables() ([]string, error) {
	query := "SHOW TABLES"
	rows, err := dm.sourceDB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to get tables: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %v", err)
		}

		// Skip tables if they're in the skip list
		skip := false
		for _, skipTable := range dm.config.SkipTables {
			if tableName == skipTable {
				skip = true
				break
			}
		}

		if !skip {
			tables = append(tables, tableName)
		}
	}

	return tables, nil
}

// GetTableSchema retrieves the CREATE TABLE statement for a table
func (dm *DatabaseMigrator) GetTableSchema(tableName string) (string, error) {
	query := fmt.Sprintf("SHOW CREATE TABLE `%s`", tableName)
	var table, createStmt string

	err := dm.sourceDB.QueryRow(query).Scan(&table, &createStmt)
	if err != nil {
		return "", fmt.Errorf("failed to get schema for table %s: %v", tableName, err)
	}

	return createStmt, nil
}

// CreateTable creates a table in the destination database
func (dm *DatabaseMigrator) CreateTable(createStmt string) error {
	_, err := dm.destDB.Exec(createStmt)
	if err != nil {
		return fmt.Errorf("failed to create table: %v", err)
	}
	return nil
}

// GetTableRowCount gets the total number of rows in a table
func (dm *DatabaseMigrator) GetTableRowCount(tableName string) (int, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName)
	var count int
	err := dm.sourceDB.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get row count for table %s: %v", tableName, err)
	}
	return count, nil
}

// GetTableColumns retrieves column names for a table
func (dm *DatabaseMigrator) GetTableColumns(tableName string) ([]string, error) {
	query := fmt.Sprintf("SHOW COLUMNS FROM `%s`", tableName)
	rows, err := dm.sourceDB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns for table %s: %v", tableName, err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var field, typ, null, key, defaultVal, extra sql.NullString
		if err := rows.Scan(&field, &typ, &null, &key, &defaultVal, &extra); err != nil {
			return nil, fmt.Errorf("failed to scan column info: %v", err)
		}
		columns = append(columns, field.String)
	}

	return columns, nil
}

// GetTableForeignKeys retrieves foreign key information for a table
func (dm *DatabaseMigrator) GetTableForeignKeys(tableName string) ([]ForeignKeyInfo, error) {
	query := `
		SELECT 
			COLUMN_NAME,
			REFERENCED_TABLE_NAME,
			REFERENCED_COLUMN_NAME
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
		WHERE TABLE_SCHEMA = ? 
		AND TABLE_NAME = ? 
		AND REFERENCED_TABLE_NAME IS NOT NULL`

	rows, err := dm.sourceDB.Query(query, dm.config.Source.Database, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get foreign keys for table %s: %v", tableName, err)
	}
	defer rows.Close()

	var foreignKeys []ForeignKeyInfo
	for rows.Next() {
		var columnName, referencedTable, referencedColumn sql.NullString
		if err := rows.Scan(&columnName, &referencedTable, &referencedColumn); err != nil {
			return nil, fmt.Errorf("failed to scan foreign key info: %v", err)
		}

		if referencedTable.Valid && referencedColumn.Valid {
			foreignKeys = append(foreignKeys, ForeignKeyInfo{
				TableName:        tableName,
				ColumnName:       columnName.String,
				ReferencedTable:  referencedTable.String,
				ReferencedColumn: referencedColumn.String,
			})
		}
	}

	return foreignKeys, nil
}

// SortTablesByDependencies sorts tables so that tables without dependencies come first
func (dm *DatabaseMigrator) SortTablesByDependencies(tables []string) ([]string, error) {
	// Build dependency map
	dependencies := make(map[string][]string)
	allForeignKeys := make(map[string][]ForeignKeyInfo)

	// Get foreign keys for all tables
	for _, tableName := range tables {
		fks, err := dm.GetTableForeignKeys(tableName)
		if err != nil {
			return nil, err
		}
		allForeignKeys[tableName] = fks

		var deps []string
		for _, fk := range fks {
			// Skip self-referencing foreign keys (they don't prevent table creation)
			if fk.ReferencedTable == tableName {
				continue
			}

			// Only include dependencies that are in our table list
			for _, t := range tables {
				if t == fk.ReferencedTable {
					deps = append(deps, fk.ReferencedTable)
					break
				}
			}
		}
		dependencies[tableName] = deps
	}

	// Topological sort
	var result []string
	visited := make(map[string]bool)
	temp := make(map[string]bool)

	var visit func(string) error
	visit = func(tableName string) error {
		if temp[tableName] {
			return fmt.Errorf("circular dependency detected involving table %s", tableName)
		}
		if visited[tableName] {
			return nil
		}

		temp[tableName] = true

		for _, dep := range dependencies[tableName] {
			if err := visit(dep); err != nil {
				return err
			}
		}

		temp[tableName] = false
		visited[tableName] = true
		result = append(result, tableName)
		return nil
	}

	for _, tableName := range tables {
		if !visited[tableName] {
			if err := visit(tableName); err != nil {
				return nil, err
			}
		}
	}

	return result, nil
}

// DisableForeignKeyChecks disables foreign key checks temporarily
func (dm *DatabaseMigrator) DisableForeignKeyChecks() error {
	_, err := dm.destDB.Exec("SET FOREIGN_KEY_CHECKS = 0")
	if err != nil {
		return fmt.Errorf("failed to disable foreign key checks: %v", err)
	}
	return nil
}

// EnableForeignKeyChecks re-enables foreign key checks
func (dm *DatabaseMigrator) EnableForeignKeyChecks() error {
	_, err := dm.destDB.Exec("SET FOREIGN_KEY_CHECKS = 1")
	if err != nil {
		return fmt.Errorf("failed to enable foreign key checks: %v", err)
	}
	return nil
}

// MigrateTableData migrates data from source to destination table in batches
func (dm *DatabaseMigrator) MigrateTableData(tableName string) error {
	dm.logger.Log(fmt.Sprintf("Starting data migration for table: %s", tableName))

	// Get table columns
	columns, err := dm.GetTableColumns(tableName)
	if err != nil {
		return err
	}

	columnNames := strings.Join(columns, "`, `")
	placeholders := strings.Repeat("?,", len(columns))
	placeholders = placeholders[:len(placeholders)-1] // Remove last comma

	// Get total row count
	totalRows, err := dm.GetTableRowCount(tableName)
	if err != nil {
		return err
	}

	dm.logger.Log(fmt.Sprintf("Table %s has %d rows to migrate", tableName, totalRows))

	if totalRows == 0 {
		dm.logger.Log(fmt.Sprintf("Table %s is empty, skipping data migration", tableName))
		return nil
	}

	// Prepare insert statement
	insertQuery := fmt.Sprintf("INSERT INTO `%s` (`%s`) VALUES (%s)",
		tableName, columnNames, placeholders)

	insertStmt, err := dm.destDB.Prepare(insertQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %v", err)
	}
	defer insertStmt.Close()

	// Migrate data in batches
	offset := 0
	migratedRows := 0

	for offset < totalRows {
		selectQuery := fmt.Sprintf("SELECT `%s` FROM `%s` LIMIT %d OFFSET %d",
			columnNames, tableName, dm.config.BatchSize, offset)

		rows, err := dm.sourceDB.Query(selectQuery)
		if err != nil {
			return fmt.Errorf("failed to select data from table %s: %v", tableName, err)
		}

		// Process batch
		for rows.Next() {
			// Create slice to hold values
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for i := range values {
				valuePtrs[i] = &values[i]
			}

			// Scan values
			if err := rows.Scan(valuePtrs...); err != nil {
				rows.Close()
				return fmt.Errorf("failed to scan row: %v", err)
			}

			// Process values to handle invalid dates and other problematic values
			for i, val := range values {
				if val != nil {
					colName := columns[i]
					switch v := val.(type) {
					case string:
						if v == "0000-00-00" || v == "0000-00-00 00:00:00" {
							if tableName == "AspNetUsers" && colName == "Birthday" {
								values[i] = "1970-01-01"
							} else {
								values[i] = nil
							}
						}
					case []byte:
						str := string(v)
						if str == "0000-00-00" || str == "0000-00-00 00:00:00" {
							if tableName == "AspNetUsers" && colName == "Birthday" {
								values[i] = "1970-01-01"
							} else {
								values[i] = nil
							}
						}
					case time.Time:
						if v.IsZero() || v.Year() == 0 {
							if tableName == "AspNetUsers" && colName == "Birthday" {
								values[i] = "1970-01-01"
							} else {
								values[i] = nil
							}
						}
					}
				}
			}

			// Insert into destination
			_, err := insertStmt.Exec(values...)
			if err != nil {
				rows.Close()
				return fmt.Errorf("failed to insert row: %v", err)
			}

			migratedRows++
		}

		rows.Close()
		offset += dm.config.BatchSize

		// Log progress
		progress := float64(migratedRows) / float64(totalRows) * 100
		dm.logger.Log(fmt.Sprintf("Table %s: %d/%d rows migrated (%.2f%%)",
			tableName, migratedRows, totalRows, progress))
	}

	dm.logger.Log(fmt.Sprintf("Completed data migration for table: %s (%d rows)", tableName, migratedRows))
	return nil
}

// MigrateTable migrates both schema and data for a single table
func (dm *DatabaseMigrator) MigrateTable(tableName string) error {
	dm.logger.Log(fmt.Sprintf("Starting migration for table: %s", tableName))

	// Get and create table schema
	createStmt, err := dm.GetTableSchema(tableName)
	if err != nil {
		return err
	}

	if err := dm.CreateTable(createStmt); err != nil {
		return fmt.Errorf("failed to create table %s: %v", tableName, err)
	}

	dm.logger.Log(fmt.Sprintf("Created table schema for: %s", tableName))

	// Migrate table data
	if err := dm.MigrateTableData(tableName); err != nil {
		return fmt.Errorf("failed to migrate data for table %s: %v", tableName, err)
	}

	return nil
}

// Migrate performs the complete database migration
func (dm *DatabaseMigrator) Migrate() error {
	dm.logger.Log("Starting database migration")
	startTime := time.Now()

	// Get all tables
	tables, err := dm.GetTables()
	if err != nil {
		return fmt.Errorf("failed to get tables: %v", err)
	}

	dm.logger.Log(fmt.Sprintf("Found %d tables to migrate: %v", len(tables), tables))

	// Sort tables by dependencies
	dm.logger.Log("Analyzing table dependencies...")
	sortedTables, err := dm.SortTablesByDependencies(tables)
	if err != nil {
		return fmt.Errorf("failed to sort tables by dependencies: %v", err)
	}

	dm.logger.Log(fmt.Sprintf("Tables sorted by dependencies: %v", sortedTables))

	// Disable foreign key checks during migration
	dm.logger.Log("Disabling foreign key checks for migration...")
	if err := dm.DisableForeignKeyChecks(); err != nil {
		return fmt.Errorf("failed to disable foreign key checks: %v", err)
	}

	// Migrate each table in dependency order
	for i, tableName := range sortedTables {
		dm.logger.Log(fmt.Sprintf("Migrating table %d/%d: %s", i+1, len(sortedTables), tableName))

		if err := dm.MigrateTable(tableName); err != nil {
			// Re-enable foreign key checks before returning error
			dm.EnableForeignKeyChecks()
			return fmt.Errorf("migration failed for table %s: %v", tableName, err)
		}
	}

	// Re-enable foreign key checks
	dm.logger.Log("Re-enabling foreign key checks...")
	if err := dm.EnableForeignKeyChecks(); err != nil {
		return fmt.Errorf("failed to enable foreign key checks: %v", err)
	}

	duration := time.Since(startTime)
	dm.logger.Log(fmt.Sprintf("Database migration completed successfully in %v", duration))
	return nil
}

func main() {
	config := MigrationConfig{
		Source: DatabaseConfig{
			Host:     "localhost",
			Port:     "3306",
			Username: "root",
			Password: "",
			Database: "lms_dev",
		},
		Destination: DatabaseConfig{
			Host:     "localhost",
			Port:     "3306",
			Username: "root",
			Password: "",
			Database: "lms_dev",
		},
		BatchSize:  1000,
		SkipTables: []string{},
		LogFile:    "migration.log",
	}

	migrator, err := NewDatabaseMigrator(config)
	if err != nil {
		log.Fatalf("Failed to create migrator: %v", err)
	}
	defer migrator.Close()

	if err := migrator.Migrate(); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}

	fmt.Println("Migration completed successfully!")
}
