package main

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	"github.com/nu7hatch/gouuid"
	"golang.org/x/net/context"
	metadata "google.golang.org/grpc/metadata"
)

// TODO(labkode) set collation for table and column to utf8. The default is swedish
type record struct {
	ID       string
	Path     string `sql:"unique_index:idx_path"`
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
func newGRPCTraceContext(ctx context.Context, trace string) context.Context {
	md := metadata.Pairs("trace", trace)
	ctx = metadata.NewContext(ctx, md)
	return ctx
}

func getGRPCTraceID(ctx context.Context) (string, error) {

	md, ok := metadata.FromContext(ctx)
	if !ok {
		id, err := uuid.NewV4()
		if err != nil {
			return "", err
		}
		return id.String(), nil
	}

	tokens := md["trace"]
	if len(tokens) == 0 {
		id, err := uuid.NewV4()
		if err != nil {
			return "", err
		}
		return id.String(), nil
	}

	if tokens[0] != "" {
		return tokens[0], nil
	}

	id, err := uuid.NewV4()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}
