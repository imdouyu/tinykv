package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	response := &kvrpcpb.RawGetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return response, err
	}
	defer reader.Close()
	response.Value, err = reader.GetCF(req.Cf, req.Key)
	if response.Value == nil {
		return &kvrpcpb.RawGetResponse{NotFound: true}, err
	}
	if err != nil {
		return response, err
	}
	return response, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	response := &kvrpcpb.RawPutResponse{}
	data := []storage.Modify{{Data: storage.Put{Cf: req.Cf, Key: req.Key, Value: req.Value}}}
	err := server.storage.Write(req.Context, data)
	if err != nil {
		return nil, err
	}
	return response, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	response := &kvrpcpb.RawDeleteResponse{}
	data := []storage.Modify{{Data: storage.Delete{Cf: req.Cf, Key: req.Key}}}
	err := server.storage.Write(req.Context, data)
	if err != nil {
		return nil, err
	}
	return response, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	response := &kvrpcpb.RawScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	it := reader.IterCF(req.Cf)
	defer it.Close()
	it.Seek(req.StartKey)
	for i := uint32(0); i < req.Limit && it.Valid(); i++ {
		item := it.Item()
		key := item.Key()
		val, _ := item.Value()
		kv := kvrpcpb.KvPair{
			Key:   key,
			Value: val,
		}
		response.Kvs = append(response.Kvs, &kv)
		it.Next()
	}
	return response, err
}
