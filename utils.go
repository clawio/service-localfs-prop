package main

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
)

type record struct {
	ID       string
	Path     string
	Checksum string
	ETag     string
	MTime    uint32
}

func newDB(driver, dsn string) (*gorm.DB, error) {

	db, err := gorm.Open(driver, dsn)
	if err != nil {
		return nil, err
	}

	db.AutoMigrate(&record{})

	return &db, nil
}
