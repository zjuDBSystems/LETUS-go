package main

import (
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
	// os.RemoveAll(defaultConfig.DataPath)

	// defaultConfig.BucketMode = false
	db, err := letus.Open(defaultConfig, letus.DefaultLogger{})
	if err != nil {
		panic(err)
	}
	value := random.RandBytes(1024)

	total := 20

	prefix := "-account"
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

		if err := tx.Write(uint64(i)); err != nil {
			panic(err)
		}

		if i != 0 && i%10 == 0 {
			if err := db.Commit(uint64(i)); err != nil {
				panic(err)
			}
		}
	}
}
