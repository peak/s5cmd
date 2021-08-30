package transfer

import (
	"context"

	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
)

func (m *Manager) PrepareUploadTask(ctx context.Context, srcurl, dsturl *url.URL) func() error {
	return func() error {
		dsturl_local := m.PrepareRemoteDestination(srcurl, dsturl)
		err := m.doUpload(ctx, srcurl, dsturl_local)
		return ReturnError(err, "upload", srcurl, dsturl_local)
	}
}

func (m *Manager) doUpload(ctx context.Context, srcurl, dsturl *url.URL) error {
	srcClient := storage.NewLocalClient(m.storageOptions)

	file, err := srcClient.Open(srcurl.Absolute())
	if err != nil {
		return err
	}
	defer file.Close()

	dstClient, err := storage.NewRemoteClient(ctx, dsturl, m.storageOptions)
	if err != nil {
		return err
	}

	metadata := storage.NewMetadata()

	err = dstClient.Put(ctx, file, dsturl, metadata, m.concurrency, m.partSize)
	if err != nil {
		return err
	}

	obj, _ := srcClient.Stat(ctx, srcurl)
	size := obj.Size

	msg := log.InfoMessage{
		Operation:   "upload",
		Source:      srcurl,
		Destination: dsturl,
		Object: &storage.Object{
			Size: size,
		},
	}
	log.Info(msg)

	return nil
}
