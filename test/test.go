package test

import (
	"encoding/csv"
	"io"
	"log"
	"math/big"
	"os"
	"strconv"
	"time"
)

var ShardNum = 4

var FromAddLoc = make(map[string]int)

var ToAddLoc = make(map[string]int)

var FromAddLoc_tocsv [][]string

var ToAddLoc_tocsv [][]string

// Test 每个address最一开始的位置
func Test() {

	i := 0
	var from_address []string
	var to_address []string
	var blockTimeStamp []string
	var UnixTime []int64
	loc, _ := time.LoadLocation("Local")

	// 读取文件
	fileName := "../bq-results-20190905-154154-u51yqnfufcbn.csv"
	fs, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("can not open the file, err is %+v", err)
	}
	defer fs.Close()

	r := csv.NewReader(fs)
	//针对大文件，一行一行的读取文件
	for {
		row, err := r.Read()
		if err != nil && err != io.EOF {
			log.Fatalf("can not read, err is %+v", err)
		}
		if err == io.EOF {
			break
		}
		// fmt.Println(row)

		if i != 0 {
			from_address = append(from_address, row[1])
			to_address = append(to_address, row[2])
			blockTimeStamp = append(blockTimeStamp, row[6][0:len(row[6])-4])

			// 字符串转Unix时间
			tm, _ := time.ParseInLocation("2006-01-02 15:04:05", blockTimeStamp[i-1], loc)
			// fmt.Println(tm.Unix())
			UnixTime = append(UnixTime, tm.Unix())
		}

		i++

	}

	BigShardNum := big.NewInt(int64(ShardNum))

	for i, s := range from_address {
		_, ok := FromAddLoc[from_address[i]]
		if ok {

		} else {
			// v, _ := strconv.ParseInt(s, 0, 64)
			v := new(big.Int)
			v.SetString(s, 0)
			// fmt.Println(v)
			v.Mod(v, BigShardNum)
			// fmt.Println(v)
			v_int := v.Int64()
			// fmt.Println(v_int)
			FromAddLoc[s] = int(v_int)
			var FromAddLoc_eachrow []string
			FromAddLoc_eachrow = append(FromAddLoc_eachrow, s)
			FromAddLoc_eachrow = append(FromAddLoc_eachrow, strconv.Itoa(int(v_int)))
			FromAddLoc_tocsv = append(FromAddLoc_tocsv, FromAddLoc_eachrow)
		}
	}

	for i, s := range to_address {
		_, ok := ToAddLoc[to_address[i]]
		if ok {

		} else {
			// v, _ := strconv.ParseInt(s, 0, 64)
			v := new(big.Int)
			v.SetString(s, 0)
			// fmt.Println(v)
			v.Mod(v, BigShardNum)
			// fmt.Println(v)
			v_int := v.Int64()
			// fmt.Println(v_int)
			ToAddLoc[s] = int(v_int)
			var ToAddLoc_eachrow []string
			ToAddLoc_eachrow = append(ToAddLoc_eachrow, s)
			ToAddLoc_eachrow = append(ToAddLoc_eachrow, strconv.Itoa(int(v_int)))
			ToAddLoc_tocsv = append(ToAddLoc_tocsv, ToAddLoc_eachrow)
		}
	}

	// 写入csv文件
	f, err := os.Create("from_address_location.csv") //创建文件
	if err != nil {
		panic(err)
	}
	defer f.Close()

	w := csv.NewWriter(f) //创建一个新的写入文件流
	// WriteAll方法使用Write方法向w写入多条记录，并在最后调用Flush方法清空缓存。
	w.WriteAll(FromAddLoc_tocsv)
	w.Flush()

	// 写入csv文件
	f1, err := os.Create("to_address_location.csv") //创建文件
	if err != nil {
		panic(err)
	}
	defer f1.Close()

	w1 := csv.NewWriter(f1) //创建一个新的写入文件流
	// WriteAll方法使用Write方法向w写入多条记录，并在最后调用Flush方法清空缓存。
	w1.WriteAll(ToAddLoc_tocsv)
	w1.Flush()

}

func ReadAddLoc(fileName string) [][]string {

	var AddLoc_tocsv [][]string
	// 读取文件
	fs, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("can not open the file, err is %+v", err)
	}
	defer fs.Close()

	r := csv.NewReader(fs)
	//针对大文件，一行一行的读取文件
	for {
		row, err := r.Read()
		if err != nil && err != io.EOF {
			log.Fatalf("can not read, err is %+v", err)
		}
		if err == io.EOF {
			break
		}
		// fmt.Println(row)

		AddLoc_tocsv = append(AddLoc_tocsv, row)

	}

	return AddLoc_tocsv

}

func RenewAddLocToCSV(AddLoc_tocsv [][]string, AddLoc map[string]int) [][]string {

	for i := 0; i < len(AddLoc_tocsv); i++ {
		_, ok := AddLoc[AddLoc_tocsv[i][0]]
		if AddLoc[AddLoc_tocsv[i][0]] == -1 || !ok {
			AddLoc_tocsv[i] = append(AddLoc_tocsv[i], AddLoc_tocsv[i][1])
		} else {
			AddLoc_tocsv[i] = append(AddLoc_tocsv[i], strconv.Itoa(AddLoc[AddLoc_tocsv[i][0]]))
		}
	}

	return AddLoc_tocsv
}

func WriteAddLocToCSV(AddLoc_tocsv [][]string, fileName string) {
	// 写入csv文件
	f, err := os.Create(fileName) //创建文件
	if err != nil {
		panic(err)
	}
	defer f.Close()

	w := csv.NewWriter(f) //创建一个新的写入文件流
	// WriteAll方法使用Write方法向w写入多条记录，并在最后调用Flush方法清空缓存。
	w.WriteAll(AddLoc_tocsv)
	w.Flush()
}
