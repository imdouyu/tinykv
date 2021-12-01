package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	KVDBEngine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	kvDB := engine_util.CreateDB(conf.DBPath, false)
	engine := engine_util.NewEngines(kvDB, nil, conf.DBPath, "")
	return &StandAloneStorage{KVDBEngine: engine}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.KVDBEngine.Kv.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.KVDBEngine.Kv.NewTransaction(false)
	return NewStandStorageReader(txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	var err error
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			err = engine_util.PutCF(s.KVDBEngine.Kv, m.Cf(), m.Key(), m.Value())
		case storage.Delete:
			err = engine_util.DeleteCF(s.KVDBEngine.Kv, m.Cf(), m.Key())
		}
		if err != nil {
			break
		}
	}
	return err
}

type StandStorageReader struct {
	Txn *badger.Txn
}

func NewStandStorageReader(txn *badger.Txn) *StandStorageReader {
	return &StandStorageReader{Txn: txn}
}

func (r *StandStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.Txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (r *StandStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.Txn)
}

func (r *StandStorageReader) Close() {
	r.Txn.Discard()
}
