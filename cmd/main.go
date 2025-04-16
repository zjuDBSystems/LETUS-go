package main

import (
	"os"
	"fmt"
	"github.com/duke-git/lancet/v2/random"
	"github.com/zjuDBSystems/LETUS-go/letus"
	_ "net/http/pprof"
)

func main() {
	// go func() {
	// 	http.Handle("/metrics", promhttp.Handler())
	// 	err := http.ListenAndServe(":8080", nil)
	// 	if err != nil {
	// 		return
	// 	}
	// }()

	defaultConfig := letus.GetDefaultConfig()
	os.RemoveAll(defaultConfig.VidbDataPath())

	// defaultConfig.BucketMode = false
	db, err := letus.Open(defaultConfig, letus.DefaultLogger{})
	if err != nil {
		panic(err)
	}
	value := random.RandBytes(1024)

	total := 20
	seq := uint64(0)
	prefix := "-account"

	// empty check
	for i := 0; i < total; i++ {
		for j := 0; j < 500; j++ {
			realKey := fmt.Sprintf("%020d%012d", i, j)
			key := []byte(prefix + realKey)
			res, err := db.Get(key)
			if err != nil {
				fmt.Println(len(res))
			}
		}
	}

	for i := 0; i < total; i++ {
		tx, err := db.NewBatch()
		if err != nil {
			panic(err)
		}

		for j := 0; j < 500; j++ {
			realKey := fmt.Sprintf("%020d%012d", i, j)

			key := []byte(prefix + realKey)
			err := tx.Put(key, value)
			if err != nil {
				panic(err)
			}
		}
		if err = tx.Hash(uint64(i)); err != nil {
			panic(err)
		}

		if seq, err = db.GetSeqNo(); err == nil{
			fmt.Printf("after hash seq_no:%d\n", seq)
		}
		
		for j := 0; j < 500; j++ {
			realKey := fmt.Sprintf("%020d%012d", i, j)
			key := []byte(prefix + realKey)
			res, err := db.Get(key)
			if err != nil {
				fmt.Println(len(res))
			}
		}
		
		if err := tx.Write(uint64(i)); err != nil {
			panic(err)
		}
		if seq, err = db.GetSeqNo(); err == nil{
			fmt.Printf("after write seq_no:%d\n", seq)
		}
		
		if i != 0 && i%10 == 0 {
			if err := db.Commit(uint64(i)); err != nil {
				panic(err)
			}
			if seq, err = db.GetSeqNo(); err == nil{
				fmt.Printf("after commit seq_no:%d\n", seq)
			}
			if seq, err = db.GetStableSeqNo(); err == nil{
				fmt.Printf("after commit stable seq_no:%d\n", seq)
			}
		}
	}
}
