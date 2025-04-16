package letus

import "strconv"
import "fmt"

func keyInc(s []byte) []byte{
	// increase string by 1
	uintVal, err := strconv.ParseUint(string(s), 10, 64)
	if err != nil {
		fmt.Println("strIncr: ", err)
		return nil
	}
	return []byte(strconv.FormatUint(uintVal+1, 10))
}

func keyDec(s []byte) []byte{
	// increase string by 1
	uintVal, err := strconv.ParseUint(string(s), 10, 64)
	if err != nil {
		fmt.Println("strIncr: ", err)
		return nil
	}
	return []byte(strconv.FormatUint(uintVal-1, 10))
}


func strCmp(a []byte, b []byte) (bool, error) {
	// compare two strings as unsigned integers
	uintValA, err := strconv.ParseUint(string(a), 10, 64)
	if err != nil {
		return false, fmt.Errorf("strCmp: ", err)
	}
	uintValB, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		return false, fmt.Errorf("strCmp: ", err)
	}
	if uintValA < uintValB {
		return true, nil
	} else {
		return false, nil
	}
}

// type LetusIterator struct {
// 	lg i.LedgerIterator
// }

// func (it *LetusIterator) Next() bool {
// 	// TODO implement me
// 	panic("implement me")
// }

// func (it *LetusIterator) Key() interface{} {
// 	// TODO implement me
// 	panic("implement me")
// }

// func (it *LetusIterator) Value() []byte {
// 	// TODO implement me
// 	panic("implement me")
// }

// func (it *LetusIterator) Release() {
// 	// TODO implement me
// 	panic("implement me")
// }

// func (it *LetusIterator) Seek(i2 interface{}) bool {
// 	// TODO implement me
// 	panic("implement me")
// }

type LetusIterator struct {
	db          *LetusKVStorage
	current_key []byte
	begin_key []byte
	end_key []byte
}

// func NewLetusIterator(db *LetusKVStorage, begin []byte, end []byte) i.Iterator {
// 	it := &LetusIterator{
// 		lg: NewLetusLedgerIterator(db, begin, end),
// 	}
// 	return it
// }

func NewLetusIterator(db *LetusKVStorage, begin []byte, end []byte) Iterator {
	it := &LetusIterator{
		db:          db,
		current_key: begin,
		begin_key:   begin,
		end_key:     end,
	}
	return it
}

// func (it *LetusIterator) LedgerIterator() i.LedgerIterator {
// 	return it.lg
// }

func (it *LetusIterator) First() bool {
	if string(it.current_key) == string(it.begin_key){
		return true
	} else {
		return false
	}
}
func (it *LetusIterator) Last() bool {
	if string(it.current_key) == string(it.end_key){
		return true
	} else {
		return false
	}
}
func (it *LetusIterator) Prev() bool {
	return it.First()
}

func (it *LetusIterator) Error() error {
	return nil
}

func (it *LetusIterator) Next() bool {
	// get the next key
	if cmp, _ := strCmp(it.current_key, it.end_key); cmp {
		it.current_key = keyInc(it.current_key)
		return true
	}
	return false
}

func (it *LetusIterator) Key() interface{} {
	return it.current_key
}

func (it *LetusIterator) Value() []byte {
	val, err := it.db.Get(it.current_key)
	if err != nil {
		// fmt.Println("Value: ", err)
		return nil
	}
	return val
}

func (it *LetusIterator) Release() {}

func (it *LetusIterator) Seek(key interface{}) bool {

	if cmp, _ := strCmp(key.([]byte), it.end_key); cmp {
		return false
	} else if cmp, _ := strCmp(it.begin_key, key.([]byte)); cmp {
		return false
	}
	it.current_key = key.([]byte)
	return true
}
