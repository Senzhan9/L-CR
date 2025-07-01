package boltdb

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/despreston/go-craq/store"
	bolt "go.etcd.io/bbolt"
)

type opType int

const (
	opWrite opType = iota
	opCommit
)

type opRequest struct {
	typ        opType
	key        string
	val        []byte // only for write
	version    uint64
	replicaPos string // only for write
	resp       chan error
}

type Bolt struct {
	DB     *bolt.DB
	file   string
	bucket []byte

	opC       chan opRequest
	wg        sync.WaitGroup
	closed    int32
	closeOnce sync.Once
}

func New(f, b string) *Bolt {
	bolt := &Bolt{
		file:   f,
		bucket: []byte(b),
		opC:    make(chan opRequest, 1024),
	}
	bolt.wg.Add(1)
	go bolt.loop()
	return bolt
}

func (b *Bolt) loop() {
	defer b.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			log.Printf("loop panic: %v", r)
		}
	}()

	for req := range b.opC {
		var err error

		switch req.typ {
		case opWrite:
			err = b.doWrite(req.key, req.val, req.version, req.replicaPos)
		case opCommit:
			err = b.doCommit(req.key, req.version)
		default:
			err = fmt.Errorf("unknown op type: %v", req.typ)
		}

		// 尝试回应 resp（不会因为接收方未读导致卡死）
		select {
		case req.resp <- err:
		default:
			log.Printf("WARNING: response channel blocked or full for key %s (type %d), dropping result", req.key, req.typ)
		}
	}
}

func (b *Bolt) isClosed() bool {
	return atomic.LoadInt32(&b.closed) != 0
}

func (b *Bolt) Connect() error {
	DB, err := bolt.Open(b.file, 0600, nil)
	if err != nil {
		return err
	}

	err = DB.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(b.bucket))
		if err != nil {
			return fmt.Errorf("could not create root bucket: %v", err)
		}
		return nil
	})
	if err != nil {
		return err
	}
	b.DB = DB
	return nil
}

func (b *Bolt) Read(key string) (*store.Item, error) {
	var result []byte

	b.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(b.bucket)
		result = b.Get([]byte(key))
		return nil
	})

	if result == nil {
		return nil, store.ErrNotFound
	}

	items, err := store.DecodeMany(result)
	if err != nil {
		return nil, err
	}

	if !items[len(items)-1].Committed {
		return nil, store.ErrDirtyItem
	}

	return items[len(items)-1], nil
}

func (b *Bolt) Write(key string, val []byte, version uint64, replicaPos string) error {
	resp := make(chan error, 1)
	return b.sendOp(opRequest{
		typ:        opWrite,
		key:        key,
		val:        val,
		version:    version,
		replicaPos: replicaPos,
		resp:       resp,
	})
}

func (b *Bolt) Commit(key string, version uint64) error {
	resp := make(chan error, 1)
	return b.sendOp(opRequest{
		typ:     opCommit,
		key:     key,
		version: version,
		resp:    resp,
	})
}

func (b *Bolt) sendOp(req opRequest) error {
	if b.isClosed() {
		return fmt.Errorf("bolt db is closed")
	}

	select {
	case b.opC <- req:
		select {
		case err := <-req.resp:
			return err
		case <-time.After(3 * time.Second):
			return fmt.Errorf("timeout waiting for response for key %s", req.key)
		}
	case <-time.After(3 * time.Second):
		return fmt.Errorf("timeout sending request for key %s", req.key)
	}
}

func (b *Bolt) doWrite(key string, val []byte, version uint64, replicaPos string) error {
	var v []*store.Item
	k := []byte(key)

	item := &store.Item{
		Committed:  false,
		Value:      val,
		Version:    version,
		Key:        key,
		ReplicaPos: replicaPos,
	}

	return b.DB.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(b.bucket)
		existing := bucket.Get(k)

		if existing == nil {
			v = append(v, item)
		} else {
			items, err := store.DecodeMany(existing)
			if err != nil {
				log.Printf("Write() decoding error\n  %#v", err)
				return err
			}
			v = append(items, item)
		}

		encoded, err := store.Encode(v)
		if err != nil {
			log.Printf("Write() encode error\n  %#v", err)
			return err
		}

		return bucket.Put(k, encoded)
	})
}

func (b *Bolt) doCommit(key string, version uint64) error {
	const maxRetries = 10
	const retryInterval = 300 * time.Millisecond

	k := []byte(key)

	var lastErr error

	for i := 0; i < maxRetries; i++ {
		err := b.DB.Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(b.bucket)
			result := bucket.Get(k)
			if result == nil {
				return store.ErrNotFound
			}

			items, err := store.DecodeMany(result)
			if err != nil {
				return err
			}

			var found bool
			var newItems []*store.Item
			for _, itm := range items {
				if itm.Version < version {
					continue
				}
				if itm.Version == version {
					itm.Committed = true
					found = true
				}
				newItems = append(newItems, itm)
			}

			if !found {
				return store.ErrNotFound
			}

			encoded, err := store.Encode(newItems)
			if err != nil {
				return err
			}
			return bucket.Put(k, encoded)
		})

		if err == nil {
			return nil // 成功了
		}

		if errors.Is(err, store.ErrNotFound) {
			lastErr = err
			time.Sleep(retryInterval)
			continue
		}

		return err // 非 NotFound 错误，立即返回
	}

	return fmt.Errorf("Commit retry failed for key %s version %d: %v", key, version, lastErr)
}

func (b *Bolt) Close() error {
	b.closeOnce.Do(func() {
		atomic.StoreInt32(&b.closed, 1)
		close(b.opC) // 触发 loop 退出
		b.wg.Wait()
		if b.DB != nil {
			b.DB.Close()
		}
	})
	return nil
}

func (b *Bolt) ReadVersion(key string, version uint64) (*store.Item, error) {
	var result []byte

	b.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(b.bucket)
		result = b.Get([]byte(key))
		return nil
	})

	if result == nil {
		return nil, store.ErrNotFound
	}

	items, err := store.DecodeMany(result)
	if err != nil {
		return nil, err
	}

	for _, item := range items {
		if item.Version == version {
			return item, nil
		}
	}

	return nil, store.ErrNotFound
}

func (b *Bolt) AllNewerCommitted(verByKey map[string]uint64) ([]*store.Item, error) {
	newer := []*store.Item{}

	err := b.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(b.bucket)
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			items, err := store.DecodeMany(v)
			if err != nil {
				log.Printf("Error decoding items for key %s in AllNewerCommitted\n", k)
				return err
			}

			newest := items[len(items)-1]
			highestVer, has := verByKey[string(k)]
			if newest.Committed && (!has || newest.Version > highestVer) {
				newer = append(newer, newest)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}
	return newer, nil
}

func (b *Bolt) AllNewerDirty(verByKey map[string]uint64) ([]*store.Item, error) {
	newer := []*store.Item{}

	err := b.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(b.bucket)
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			items, err := store.DecodeMany(v)
			if err != nil {
				log.Printf("Error decoding items for key %s in AllNewerDirty\n", k)
				return err
			}

			newest := items[len(items)-1]
			highestVer, has := verByKey[string(k)]
			if !has || !newest.Committed && newest.Version > highestVer {
				newer = append(newer, newest)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}
	return newer, nil
}

func (b *Bolt) AllDirty() ([]*store.Item, error) {
	dirty := []*store.Item{}

	err := b.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(b.bucket)
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			items, err := store.DecodeMany(v)
			if err != nil {
				log.Printf("Error decoding items for key %s in AllDirty\n", k)
				return err
			}

			for _, item := range items {
				if !item.Committed {
					dirty = append(dirty, item)
				}
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}
	return dirty, nil
}

func (b *Bolt) AllCommitted() ([]*store.Item, error) {
	committed := []*store.Item{}

	err := b.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(b.bucket)
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			items, err := store.DecodeMany(v)
			if err != nil {
				log.Printf("Error decoding items for key %s in AllCommitted\n", k)
				return err
			}

			for _, item := range items {
				if item.Committed {
					committed = append(committed, item)
				}
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}
	return committed, nil
}
