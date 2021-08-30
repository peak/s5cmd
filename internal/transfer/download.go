package transfer

import (
	"context"

	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
)

// PrepareDownloadTask returns a function which handles download operation.
func (m *Manager) PrepareDownloadTask(ctx context.Context, srcurl, dsturl *url.URL) func() error {
	return func() error {
		dsturl_local, err := m.prepareLocalDestination(ctx, srcurl, dsturl)
		if err != nil {
			return err
		}
		err = m.doDownload(ctx, srcurl, dsturl_local)
		return ReturnError(err, "download", srcurl, dsturl_local)
	}
}

func (m *Manager) doDownload(ctx context.Context, srcurl, dsturl *url.URL) error {
	srcClient, err := storage.NewRemoteClient(ctx, srcurl, m.storageOptions)
	if err != nil {
		return err
	}

	dstClient := storage.NewLocalClient(m.storageOptions)

	file, err := dstClient.Create(dsturl.Absolute())
	if err != nil {
		return err
	}
	defer file.Close()

	size, err := srcClient.Get(ctx, srcurl, file, m.concurrency, m.partSize)
	if err != nil {
		_ = dstClient.Delete(ctx, dsturl)
		return err
	}

	msg := log.InfoMessage{
		Operation:   "download",
		Source:      srcurl,
		Destination: dsturl,
		Object: &storage.Object{
			Size: size,
		},
	}
	log.Info(msg)

	return nil
}
