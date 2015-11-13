package main

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
)

// TODO(labkode) set collation for table and column to utf8. The default is swedish
type record struct {
	ID       string
	Path     string
	Checksum string
	ETag     string
	MTime    uint32
}

func (r *record) String() string {
	return fmt.Sprintf("id=%s path=%s sum=%s etag=%s mtime=%d",
		r.ID, r.Path, r.Checksum, r.ETag, r.MTime)
}
func newDB(driver, dsn string) (*gorm.DB, error) {

	db, err := gorm.Open(driver, dsn)
	if err != nil {
		return nil, err
	}

	db.AutoMigrate(&record{})

	return &db, nil
}
