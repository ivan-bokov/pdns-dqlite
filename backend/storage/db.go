package storage

import "database/sql"

type Storage struct {
	db *sql.DB
}
