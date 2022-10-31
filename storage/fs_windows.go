//go:build windows

package storage

import (
	"os"
	"syscall"
	"time"
)

func GetFileTime(filename string) (time.Time, time.Time, time.Time, error) {
	fi, err := os.Stat(filename)
	if err != nil {
		return time.Time{}, time.Time{}, time.Time{}, err
	}

	d := fi.Sys().(*syscall.Win32FileAttributeData)
	cTime := time.Unix(0, d.CreationTime.Nanoseconds())
	aTime := time.Unix(0, d.LastAccessTime.Nanoseconds())

	mTime := fi.ModTime()

	return aTime, mTime, cTime, nil
}

func SetFileTime(filename string, accessTime, modificationTime, creationTime time.Time) error {
	var err error
	if accessTime.IsZero() && modificationTime.IsZero() && creationTime.IsZero() {
		// Nothing recorded in s3. Return fast.
		return nil
	} else if accessTime.IsZero() {
		accessTime, _, _, err = GetFileTime(filename)
		if err != nil {
			return err
		}
	} else if modificationTime.IsZero() {
		_, modificationTime, _, err = GetFileTime(filename)
		if err != nil {
			return err
		}
	} else if creationTime.IsZero() {
		_, _, creationTime, err = GetFileTime(filename)
		if err != nil {
			return err
		}
	}

	aft := syscall.NsecToFiletime(accessTime.UnixNano())
	mft := syscall.NsecToFiletime(modificationTime.UnixNano())
	cft := syscall.NsecToFiletime(creationTime.UnixNano())

	fd, err := syscall.Open(filename, os.O_RDWR, 0775)
	if err != nil {
		return err
	}
	err = syscall.SetFileTime(fd, &cft, &aft, &mft)

	defer syscall.Close(fd)

	if err != nil {
		return err
	}
	return nil
}
