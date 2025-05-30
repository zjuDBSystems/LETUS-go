package letus

import (
    "crypto/sha1"
    "encoding/hex"
	"math"
	"github.com/pkg/errors"
	"unsafe"
	"github.com/zjuDBSystems/LETUS-go/types"
)
/*
#cgo CFLAGS: -I${SRCDIR}
#cgo LDFLAGS: -L${SRCDIR} -lletus -lssl -lcrypto -lstdc++
#include "Letus.h"
#include <stdio.h>
#include <stdlib.h>
*/
import "C"

func sha1hash(key []byte) []byte { 
	h := sha1.New()
	h.Write(key)
	keyhash := h.Sum(nil)
	keyhashstr := []byte("0" + hex.EncodeToString(keyhash))
	return keyhashstr
}

func getCPtr(data []byte) *C.char {
	return (*C.char)(unsafe.Pointer(&[]byte(string(data))[0]))
}

// LetusKVStorage is an implementation of KVStorage.
type LetusKVStorage struct {
	c *C.Letus
	tid uint64
	stable_seq_no uint64
	current_seq_no uint64
	hashed_seq_no  uint64
	logger DefaultLogger
}

func Open(config VidbConfigInterface, logger DefaultLogger) (KVStorage, error) {
	path := config.VidbDataPath()
	s := &LetusKVStorage{
		c:              C.OpenLetus(C.CString(path)),
		tid:            0,
		stable_seq_no:  math.MaxUint64,
		current_seq_no: math.MaxUint64,
		hashed_seq_no:  math.MaxUint64,
		logger:         logger,
	}
	return s, nil
}

func (s *LetusKVStorage) Put(key []byte, value []byte) error {
	seq := uint64(1)
	if s.current_seq_no != math.MaxUint64 {
		seq = s.current_seq_no + 1
	}
	sha1key := sha1hash(key)
	C.LetusPut(s.c, C.uint64_t(s.tid), C.uint64_t(seq), getCPtr(sha1key), getCPtr(value))
	s.logger.Infof("Letus Put! tid=%d, seq=%d, key=%s(%s)\n", s.tid, seq, string(key), string(sha1key))
	// s.logger.Infof("Letus Put! tid=%d, seq=%d, key=%s(%s), value=%s\n", s.tid, seq, string(key), string(sha1key), string(value))
	return nil
}

func (s *LetusKVStorage) Get(key []byte) ([]byte, error) {
	if s.hashed_seq_no == math.MaxUint64 {
		return nil, errors.New("db not found")
	}
	seq := s.hashed_seq_no + 1
	var value *C.char
	sha1key := sha1hash(key)

	value = C.LetusGet(s.c, C.uint64_t(s.tid), C.uint64_t(seq), getCPtr(sha1key))
	// s.logger.Infof("Letus Get! tid=%d, seq=%d, key=%s(%s), value=%s\n", s.tid, seq, string(key), string(sha1key), C.GoString(value))
	s.logger.Infof("Letus Get! tid=%d, seq=%d, key=%s(%s)\n", s.tid, seq, string(key), string(sha1key))

		
	if value == nil || C.GoString(value) == "" {
		return nil, errors.New("db not found")
	}
	return []byte(C.GoString(value)), nil
}

func (s *LetusKVStorage) Delete(key []byte) error {
	seq := s.current_seq_no + 1
	sha1key := sha1hash(key)
	C.LetusDelete(s.c, C.uint64_t(s.tid), C.uint64_t(seq), getCPtr(sha1key))
	s.logger.Infof("Letus Delete! tid=%d, seq=%d, key=%s(%s)\n", s.tid, seq, string(key), string(sha1key))
	return nil 
}

func (s* LetusKVStorage) Revert(seq_ uint64) error {
	seq := seq_ + 1 
	s.logger.Infof("Letus revert! version=%d\n", seq)
	C.LetusRevert(s.c, C.uint64_t(s.tid), C.uint64_t(seq))
	s.stable_seq_no = seq_
	s.current_seq_no = seq_
	return nil 
}

func (s* LetusKVStorage) CalcRootHash(seq_ uint64) error { 
	seq := seq_ + 1
	s.logger.Infof("Letus calculate root hash! version=%d\n", seq)
	C.LetusCalcRootHash(s.c, C.uint64_t(s.tid), C.uint64_t(seq))
	s.hashed_seq_no = seq_
	return nil
}

func (s* LetusKVStorage) Write(seq_ uint64) error { 
	seq := seq_ + 1
	s.logger.Infof("Letus flush! version=%d\n", seq)
	C.LetusFlush(s.c, C.uint64_t(s.tid), C.uint64_t(seq))
	s.current_seq_no = seq_ + 1
	return nil 
}

func (s* LetusKVStorage) Commit(seq_ uint64) error { 
	seq := seq_ + 1
	s.logger.Infof("Letus commit! version=%d\n", seq)
	C.LetusFlush(s.c, C.uint64_t(s.tid), C.uint64_t(seq))
	s.stable_seq_no = seq_
	return nil 
}

func (s *LetusKVStorage) Close() error {
	s.logger.Infof("close Letus!")
	return nil 
}

func (s *LetusKVStorage) NewBatch() (Batch, error) {
	return NewLetusBatch(s)
}

func (s *LetusKVStorage) NewBatchWithEngine() (Batch, error) {
	return NewLetusBatch(s)
}

func (s *LetusKVStorage) NewIterator(begin, end []byte) Iterator {
	return NewLetusIterator(s, begin, end)
}

func (s *LetusKVStorage) GetStableSeqNo() (uint64, error) {
	return s.stable_seq_no, nil
}
func (s *LetusKVStorage) GetSeqNo() (uint64, error) {
	return s.current_seq_no, nil
}


func (s *LetusKVStorage) Proof(key []byte, seq_ uint64) (types.ProofPath, error){
	seq := seq_ + 1
	sha1key := sha1hash(key)
	proof_path_c := C.LetusProof(s.c, C.uint64_t(s.tid), C.uint64_t(seq), getCPtr(sha1key))
	proof_path_size := C.LetusGetProofPathSize(proof_path_c)
	proof_path := make(types.ProofPath, proof_path_size)
	for i:=0; i < int(proof_path_size); i++ {
		proof_node_size := C.LetusGetProofNodeSize(proof_path_c, C.uint64_t(i))
		proof_path[i] = &types.ProofNode{
			IsData: bool(C.LetusGetProofNodeIsData(proof_path_c, C.uint64_t(i))),
			Hash: []byte(C.GoString(C.LetusGetProofNodeHash(proof_path_c, C.uint64_t(i)))),
			Key: []byte(C.GoString(C.LetusGetProofNodeKey(proof_path_c, C.uint64_t(i)))),
			Index: int(C.LetusGetProofNodeIndex(proof_path_c, C.uint64_t(i))),
			Inodes: make(types.Inodes, proof_node_size),
		}
		for j:=0; j < int(proof_node_size); j++ {
			proof_path[i].Inodes[j] = &types.Inode{
				Hash: []byte(C.GoString(C.LetusGetINodeHash(proof_path_c, C.uint64_t(i), C.uint64_t(j)))),
				Key: []byte(C.GoString(C.LetusGetINodeKey(proof_path_c, C.uint64_t(i), C.uint64_t(j)))),
			}
		}
	}
	return proof_path, nil
}

// func (s *LetusKVStorage) SetEngine(engine cryptocom.Engine) {}
func (s *LetusKVStorage) FSync(seq uint64) error { return nil }


type LetusConfig struct {
	DataPath      string
	CheckInterval uint64
	Compress      bool
	Encrypt       bool
	BucketMode    bool
	VlogSize      uint64
	sync          bool
}


func GetDefaultConfig() VidbConfigInterface {
	DefaultSync := false
	DefaultEncryption := false
	DefaultCheckInterval := uint64(100)
	DefaultDataPath := "./data"
	DefaultBucketMode := false
	DefaultVlogSize := uint64(1024 * 1024)
	return &LetusConfig{
		sync:          DefaultSync,
		Encrypt:       DefaultEncryption,
		Compress:      true,
		CheckInterval: DefaultCheckInterval,
		DataPath:      DefaultDataPath,
		BucketMode:    DefaultBucketMode,
		VlogSize:      DefaultVlogSize,
	}
}

func (v *LetusConfig) Sync() bool{
	return v.sync
}

func (v *LetusConfig) VidbDataPath() string {
	return v.DataPath
}

func (v *LetusConfig) CompressEnable() bool {
	return v.Compress
}

func (v *LetusConfig) GetBucketMode() bool {
	return v.BucketMode
}

func (v *LetusConfig) GetEncrypt() bool {
	return v.Encrypt
}

func (v *LetusConfig) GetCheckInterval() uint64 {
	return v.CheckInterval
}

func (v *LetusConfig) GetVlogSize() uint64 {
	return v.VlogSize
}