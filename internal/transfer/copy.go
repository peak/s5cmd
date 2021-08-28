package transfer

import (
	"context"

	"github.com/peak/s5cmd/log"
	"github.com/peak/s5cmd/storage"
	"github.com/peak/s5cmd/storage/url"
)

func (m *Manager) PrepareCopyTask(ctx context.Context, srcurl, dsturl *url.URL) func() error {
	return func() error {
		dsturl_local := m.prepareRemoteDestination(srcurl, dsturl)
		err := m.doCopy(ctx, srcurl, dsturl_local)
		return ReturnError(err, "copy", srcurl, dsturl_local)
	}
}

func (m *Manager) doCopy(ctx context.Context, srcurl, dsturl *url.URL) error {
	dstClient, err := storage.NewClient(ctx, dsturl, m.storageOptions)
	if err != nil {
		return err
	}

	metadata := storage.NewMetadata()

	err = dstClient.Copy(ctx, srcurl, dsturl, metadata)
	if err != nil {
		return err
	}

	msg := log.InfoMessage{
		Operation:   "copy",
		Source:      srcurl,
		Destination: dsturl,
		Object: &storage.Object{
			URL: dsturl,
		},
	}
	log.Info(msg)

	return nil
}
