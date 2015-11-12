package main

import (
	"code.google.com/p/go-uuid/uuid"
	"github.com/clawio/service.auth/lib"
	pb "github.com/clawio/service.localstore.prop/proto"
	"github.com/jinzhu/gorm"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"path"
	"time"
)

const (
	dirPerm = 0755
)

var (
	unauthenticatedError = grpc.Errorf(codes.Unauthenticated, "identity not found")
	permissionDenied     = grpc.Errorf(codes.PermissionDenied, "access denied")
)

type newServerParams struct {
	dsn          string
	db           *gorm.DB
	sharedSecret string
}

func newServer(p *newServerParams) (*server, error) {

	db, err := newDB("mysql", p.dsn)
	if err != nil {
		log.Error(err)
		return nil, err
	}

	db.LogMode(true)

	err = db.AutoMigrate(&record{}).Error
	if err != nil {
		log.Error(err)
		return nil, err
	}

	log.Infof("automigration applied")

	s := &server{}
	s.p = p
	s.db = db
	return s, nil
}

type server struct {
	p  *newServerParams
	db *gorm.DB
}

func (s *server) Get(ctx context.Context, req *pb.GetReq) (*pb.Record, error) {

	idt, err := lib.ParseToken(req.AccessToken, s.p.sharedSecret)
	if err != nil {
		log.Error(err)
		return &pb.Record{}, unauthenticatedError
	}

	log.Infof("%s", idt)

	p := path.Clean(req.Path)

	log.Infof("path is %s", p)

	rec, err := s.getByPath(p)
	// TODO(labkode) If record is not found put it. This will happen on storages with DAS
	if err != nil {
		log.Error(err)
		return &pb.Record{}, err
	}

	r := &pb.Record{}
	r.Id = rec.ID
	r.Path = rec.Path
	r.Etag = rec.ETag
	r.Modified = rec.MTime
	r.Checksum = rec.Checksum
	return r, nil
}

func (s *server) Put(ctx context.Context, req *pb.PutReq) (*pb.Void, error) {

	idt, err := lib.ParseToken(req.AccessToken, s.p.sharedSecret)
	if err != nil {
		log.Error(err)
		return &pb.Void{}, unauthenticatedError
	}

	log.Infof("%s", idt)

	p := path.Clean(req.Path)

	log.Infof("path is %s", p)

	var id string
	var etag = uuid.New()
	var mtime = time.Now().Unix()

	r, err := s.getByPath(p)
	if err != nil {
		log.Error(err)
		if err == gorm.RecordNotFound {
			id = uuid.New()
		} else {
			return &pb.Void{}, err
		}
	} else {
		id = r.ID
	}

	log.Infof("record has %s", r.String())

	err = s.db.Exec(`INSERT INTO records (id,path,checksum, e_tag, m_time) VALUES (?,?,?,?,?)
  				ON DUPLICATE KEY UPDATE checksum=VALUES(checksum), e_tag=VALUES(e_tag), m_time=VALUES(m_time)`,
		id, p, req.Checksum, etag, mtime).Error

	if err != nil {
		log.Error(err)
		return &pb.Void{}, err
	}

	log.Infof("putted record into db")

	return &pb.Void{}, nil
}

func (s *server) getByPath(path string) (*record, error) {

	r := &record{}
	err := s.db.Where("path=?", path).First(r).Error
	return r, err
}
