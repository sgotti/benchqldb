package main

import (
	"database/sql"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/boltdb/bolt"
	"github.com/sgotti/benchqlbolt/pkg/kvdb"
	"github.com/sgotti/benchqlbolt/pkg/qldb"
)

const (
	defaultPathPerm = os.FileMode(0770 | os.ModeSetgid)
	defaultFilePerm = os.FileMode(0660)
)

func BenchmarkQLEmptyROTx(b *testing.B) {
	tmpdir, err := ioutil.TempDir("", "")
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}
	defer os.RemoveAll(tmpdir)
	db, err := qldb.NewDB(tmpdir)
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err = db.Do(func(tx *sql.Tx) error {
			return nil
		}); err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func BenchmarkBoltEmptyROTx(b *testing.B) {
	tmpdir, err := ioutil.TempDir("", "")
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}
	//defer os.RemoveAll(tmpdir)
	f, err := os.Create(filepath.Join(tmpdir, "db"))
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}
	f.Close()

	db := kvdb.NewDB(filepath.Join(tmpdir, "db"), defaultFilePerm)
	if err := db.DoRW(func(tx *bolt.Tx) error {
		return nil
	}); err != nil {
		b.Fatalf("unexpected error: %v", err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := db.DoRO(func(tx *bolt.Tx) error {
			return nil
		}); err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func BenchmarkQLEmptyRWTx(b *testing.B) {
	tmpdir, err := ioutil.TempDir("", "")
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}
	defer os.RemoveAll(tmpdir)
	db, err := qldb.NewDB(tmpdir)
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err = db.Do(func(tx *sql.Tx) error {
			return nil
		}); err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func BenchmarkBoltEmptyRWTx(b *testing.B) {
	tmpdir, err := ioutil.TempDir("", "")
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}
	//defer os.RemoveAll(tmpdir)
	f, err := os.Create(filepath.Join(tmpdir, "db"))
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}
	f.Close()

	db := kvdb.NewDB(filepath.Join(tmpdir, "db"), defaultFilePerm)
	if err := db.DoRW(func(tx *bolt.Tx) error {
		return nil
	}); err != nil {
		b.Fatalf("unexpected error: %v", err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := db.DoRW(func(tx *bolt.Tx) error {
			return nil
		}); err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func BenchmarkQLROTx(b *testing.B) {
	tmpdir, err := ioutil.TempDir("", "")
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}
	defer os.RemoveAll(tmpdir)
	db, err := qldb.NewDB(tmpdir)
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}
	if err = db.Do(func(tx *sql.Tx) error {
		dbCreateStmts := []string{
			"CREATE TABLE IF NOT EXISTS table01 (key string, value string);",
			"CREATE UNIQUE INDEX IF NOT EXISTS keyidx ON table01 (key);",
		}
		for _, stmt := range dbCreateStmts {
			_, err = tx.Exec(stmt)
			if err != nil {
				return err
			}
		}
		for i := 0; i < 1000; i++ {
			_, err = tx.Exec("INSERT INTO table01 VALUES ($1, $2)", "key"+strconv.Itoa(i), "value"+strconv.Itoa(i))
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatalf("unexpected error: %v", err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err = db.Do(func(tx *sql.Tx) error {
			rows, err := tx.Query("SELECT * FROM table01 WHERE key == $1", "key500")
			if err != nil {
				return err
			}
			for rows.Next() {
				var key, value string
				if err := rows.Scan(&key, &value); err != nil {
					return err
				}
				if value != "value500" {
					b.Fatalf("unexpected value: %s", value)
				}
			}
			if err := rows.Err(); err != nil {
				return err
			}

			return nil
		}); err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func BenchmarkBoltROTx(b *testing.B) {
	tmpdir, err := ioutil.TempDir("", "")
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}
	defer os.RemoveAll(tmpdir)
	db := kvdb.NewDB(filepath.Join(tmpdir, "db"), defaultFilePerm)
	if err := db.DoRW(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket([]byte("bucket01"))
		if err != nil {
			return err
		}
		for i := 0; i < 1000; i++ {
			if err := b.Put([]byte("key"+strconv.Itoa(i)), []byte("value"+strconv.Itoa(i))); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := db.DoRO(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte("bucket01"))
			if bucket == nil {
				b.Fatalf("non existent bucket")
			}
			value := bucket.Get([]byte("key500"))
			if string(value) != "value500" {
				b.Fatalf("unexpected value: %s", value)
			}
			return nil
		}); err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func BenchmarkQLRWTx(b *testing.B) {
	tmpdir, err := ioutil.TempDir("", "")
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}
	defer os.RemoveAll(tmpdir)
	db, err := qldb.NewDB(tmpdir)
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}
	if err = db.Do(func(tx *sql.Tx) error {
		dbCreateStmts := []string{
			"CREATE TABLE IF NOT EXISTS table01 (key string, value string);",
			"CREATE UNIQUE INDEX IF NOT EXISTS keyidx ON table01 (key);",
		}
		for _, stmt := range dbCreateStmts {
			_, err = tx.Exec(stmt)
			if err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		b.Fatalf("unexpected error: %v", err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err = db.Do(func(tx *sql.Tx) error {
			_, err = tx.Exec("INSERT INTO table01 VALUES ($1, $2)", "key"+strconv.Itoa(i), "value"+strconv.Itoa(i))
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}

func BenchmarkBoltRWTx(b *testing.B) {
	tmpdir, err := ioutil.TempDir("", "")
	if err != nil {
		b.Fatalf("unexpected error: %v", err)
	}
	defer os.RemoveAll(tmpdir)
	db := kvdb.NewDB(filepath.Join(tmpdir, "db"), defaultFilePerm)
	if err := db.DoRW(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("bucket01"))
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		b.Fatalf("unexpected error: %v", err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := db.DoRW(func(tx *bolt.Tx) error {
			bucket := tx.Bucket([]byte("bucket01"))
			if bucket == nil {
				b.Fatalf("non existent bucket")
			}
			if err := bucket.Put([]byte("key"+strconv.Itoa(i)), []byte("value"+strconv.Itoa(i))); err != nil {
				return err
			}
			return nil
		}); err != nil {
			b.Fatalf("unexpected error: %v", err)
		}
	}
}
