package test

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math/big"
	"os"
	"strconv"
	"time"
)

var ShardNum = 32

var FromAddLoc = make(map[string]int)

var ToAddLoc = make(map[string]int)

var FromAddLoc_tocsv [][]string

var ToAddLoc_tocsv [][]string

var CountInterval = 960000

// Test 每个address最一开始的位置
func Test() {

	var from_address []string
	var to_address []string
	// var blockTimeStamp []string
	// var UnixTime []int64
	// loc, _ := time.LoadLocation("Local")

	// 读取文件
	fileName := "../0to999999_ERC20Transaction.csv"
	from_address, to_address = ReadFile(fileName, from_address, to_address)
	fmt.Println(len(from_address))
	fileName = "../1000000to1999999_ERC20Transaction.csv"
	from_address, to_address = ReadFile(fileName, from_address, to_address)
	fmt.Println(len(from_address))
	fileName = "../2000000to2999999_ERC20Transaction.csv"
	from_address, to_address = ReadFile(fileName, from_address, to_address)
	fmt.Println(len(from_address))
	fileName = "../3000000to3999999_ERC20Transaction.csv"
	from_address, to_address = ReadFile(fileName, from_address, to_address)
	fmt.Println(len(from_address))
	fileName = "../4000000to4999999_ERC20Transaction.csv"
	from_address, to_address = ReadFile(fileName, from_address, to_address)
	fmt.Println(len(from_address))
	fileName = "../5000000to5999999_ERC20Transaction.csv"
	from_address, to_address = ReadFile(fileName, from_address, to_address)
	fmt.Println(len(from_address))
	fileName = "../6000000to6999999_ERC20Transaction.csv"
	from_address, to_address = ReadFile(fileName, from_address, to_address)
	fmt.Println(len(from_address))
	fileName = "../7000000to7999999_ERC20Transaction.csv"
	from_address, to_address = ReadFile(fileName, from_address, to_address)
	fmt.Println(len(from_address))
	fileName = "../8000000to8999999_ERC20Transaction.csv"
	from_address, to_address = ReadFile(fileName, from_address, to_address)
	fmt.Println(len(from_address))

	FromAddress := make([]string, len(from_address[32*CountInterval:33*CountInterval+576000+1]))
	copy(FromAddress, from_address[32*CountInterval:33*CountInterval+576000+1])
	ToAddress := make([]string, len(to_address[32*CountInterval:33*CountInterval+576000+1]))
	copy(ToAddress, to_address[32*CountInterval:33*CountInterval+576000+1])

	BigShardNum := big.NewInt(int64(ShardNum))

	for i, s := range FromAddress {
		_, ok := FromAddLoc[FromAddress[i]]
		if ok || s == "0x0000000000000000000000000000000000000000" {

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

	for i, s := range ToAddress {
		_, ok := ToAddLoc[ToAddress[i]]
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
	f, err := os.Create("FromLoc32Shard30720000to32256000.csv") //创建文件
	if err != nil {
		panic(err)
	}
	defer f.Close()

	w := csv.NewWriter(f) //创建一个新的写入文件流
	// WriteAll方法使用Write方法向w写入多条记录，并在最后调用Flush方法清空缓存。
	w.WriteAll(FromAddLoc_tocsv)
	w.Flush()

	// 写入csv文件
	f1, err := os.Create("ToLoc32Shard30720000to32256000.csv") //创建文件
	if err != nil {
		panic(err)
	}
	defer f1.Close()

	w1 := csv.NewWriter(f1) //创建一个新的写入文件流
	// WriteAll方法使用Write方法向w写入多条记录，并在最后调用Flush方法清空缓存。
	w1.WriteAll(ToAddLoc_tocsv)
	w1.Flush()

}

func ReadFile(fileName string, from_address, to_address []string) ([]string, []string) {
	i := 0

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

		// 去除掉coinbase
		if i != 0 && row[4] != "0x0000000000000000000000000000000000000000" {
			from_address = append(from_address, row[4])
			to_address = append(to_address, row[5])
			// blockTimeStamp = append(blockTimeStamp, row[1])

			// // 字符串转Unix时间
			// tm, _ := time.ParseInLocation("2006-01-02 15:04:05", blockTimeStamp[i-1], loc)
			// // fmt.Println(tm.Unix())
			// UnixTime = append(UnixTime, tm.Unix())
		}

		i++
		if i%10000000 == 0 {
			fmt.Println(i)
		}
	}

	return from_address, to_address
}

func ReadFileWithTime(fileName string, from_address, to_address, blockTimeStamp []string) ([]string, []string, []int64) {
	i := 0
	var UnixTime []int64
	loc, _ := time.LoadLocation("Local")

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
		if i%10000000 == 0 {
			fmt.Println(i)
		}
	}

	return from_address, to_address, UnixTime
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

func AccountDistribution() {
	// 读取address location文件
	FromAddLoc_tocsv := ReadAddLoc("FromLoc4Shard30720000to31680000.csv")

	// 变量声名
	// i := 0
	var from_address []string
	var to_address []string
	// var blockTimeStamp []string
	// var UnixTime []int64
	// loc, _ := time.LoadLocation("Local")

	var AccDistributionToCSV [][]string

	// 读取文件
	fileName := "../0to999999_ERC20Transaction.csv"
	from_address, to_address = ReadFile(fileName, from_address, to_address)
	fmt.Println(len(from_address))
	fileName = "../1000000to1999999_ERC20Transaction.csv"
	from_address, to_address = ReadFile(fileName, from_address, to_address)
	fmt.Println(len(from_address))
	fileName = "../2000000to2999999_ERC20Transaction.csv"
	from_address, to_address = ReadFile(fileName, from_address, to_address)
	fmt.Println(len(from_address))
	fileName = "../3000000to3999999_ERC20Transaction.csv"
	from_address, to_address = ReadFile(fileName, from_address, to_address)
	fmt.Println(len(from_address))
	fileName = "../4000000to4999999_ERC20Transaction.csv"
	from_address, to_address = ReadFile(fileName, from_address, to_address)
	fmt.Println(len(from_address))
	fileName = "../5000000to5999999_ERC20Transaction.csv"
	from_address, to_address = ReadFile(fileName, from_address, to_address)
	fmt.Println(len(from_address))
	fileName = "../6000000to6999999_ERC20Transaction.csv"
	from_address, to_address = ReadFile(fileName, from_address, to_address)
	fmt.Println(len(from_address))
	fileName = "../7000000to7999999_ERC20Transaction.csv"
	from_address, to_address = ReadFile(fileName, from_address, to_address)
	fmt.Println(len(from_address))
	fileName = "../8000000to8999999_ERC20Transaction.csv"
	from_address, to_address = ReadFile(fileName, from_address, to_address)
	fmt.Println(len(from_address))

	Count := 960000
	IncreaseStep := 960000

	for i := 32 * CountInterval; i < 33*CountInterval; i += IncreaseStep {
		var AccountFrequency = make(map[string]int)

		var AccDistribution = make([]int, ShardNum)

		// 计算每个账户发送交易数量，并由多到少排序
		for _, account := range from_address[i : i+Count] {
			AccountFrequency[account]++
		}

		for _, addloc := range FromAddLoc_tocsv {
			addlocint, _ := strconv.Atoi(addloc[1])
			AccDistribution[addlocint] += AccountFrequency[addloc[0]]
		}

		fmt.Println(AccDistribution)

		var AccDistributionTemp []string
		for _, value := range AccDistribution {
			AccDistributionTemp = append(AccDistributionTemp, strconv.Itoa(value))
		}
		AccDistributionToCSV = append(AccDistributionToCSV, AccDistributionTemp)
	}

	WriteAddLocToCSV(AccDistributionToCSV, "AccDistrib4Shard30720000to31680000.csv")

}

func FindCertainAccount() {
	account := "0x6690819cb98c1211a8e38790d6cd48316ed518db"
	var AccountInfo [][]string

	AccountInfo = ReadFileForFindCertainAccount("../0to999999_ERC20Transaction.csv", account, AccountInfo)
	fmt.Println(len(AccountInfo))
	AccountInfo = ReadFileForFindCertainAccount("../1000000to1999999_ERC20Transaction.csv", account, AccountInfo)
	fmt.Println(len(AccountInfo))
	AccountInfo = ReadFileForFindCertainAccount("../2000000to2999999_ERC20Transaction.csv", account, AccountInfo)
	fmt.Println(len(AccountInfo))
	AccountInfo = ReadFileForFindCertainAccount("../3000000to3999999_ERC20Transaction.csv", account, AccountInfo)
	fmt.Println(len(AccountInfo))
	AccountInfo = ReadFileForFindCertainAccount("../4000000to4999999_ERC20Transaction.csv", account, AccountInfo)
	fmt.Println(len(AccountInfo))
	AccountInfo = ReadFileForFindCertainAccount("../5000000to5999999_ERC20Transaction.csv", account, AccountInfo)
	fmt.Println(len(AccountInfo))
	AccountInfo = ReadFileForFindCertainAccount("../6000000to6999999_ERC20Transaction.csv", account, AccountInfo)
	fmt.Println(len(AccountInfo))
	AccountInfo = ReadFileForFindCertainAccount("../7000000to7999999_ERC20Transaction.csv", account, AccountInfo)
	fmt.Println(len(AccountInfo))
	AccountInfo = ReadFileForFindCertainAccount("../8000000to8999999_ERC20Transaction.csv", account, AccountInfo)
	fmt.Println(len(AccountInfo))

	// 写入csv文件
	f, err := os.Create("AccountInfoForSampled_top3rd.csv") //创建文件
	if err != nil {
		panic(err)
	}
	defer f.Close()

	w := csv.NewWriter(f) //创建一个新的写入文件流
	// WriteAll方法使用Write方法向w写入多条记录，并在最后调用Flush方法清空缓存。
	w.WriteAll(AccountInfo)
	w.Flush()
}

func ReadFileForFindCertainAccount(fileName string, account string, AccountInfo [][]string) [][]string {
	i := 0

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

		if i != 0 && account == row[4] {
			AccountInfo = append(AccountInfo, row)
		}

		i++
		if i%10000000 == 0 {
			fmt.Println(i)
		}
	}

	return AccountInfo
}

func CalRemainedTx(fileName string) {
	i := 0
	var rowlast []int
	var rowtemp int
	var FileWriteToCSV [][]string
	var Sum []string
	var loadLimit = 10 * 60 * 50 * 64

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
			var rowthis []int
			var rowdiff []int
			var filetocsvtemp []string
			var sumtemp int

			for _, value := range row {
				rowtemp, _ = strconv.Atoi(value)
				rowthis = append(rowthis, rowtemp)
			}
			for index, value := range rowthis {
				if value-rowlast[index] > 0 {
					rowdiff = append(rowdiff, value-rowlast[index])
				} else {
					rowdiff = append(rowdiff, 0)
				}
				filetocsvtemp = append(filetocsvtemp, strconv.Itoa(rowdiff[index]))
			}
			rowlast = rowthis
			FileWriteToCSV = append(FileWriteToCSV, filetocsvtemp)
			for _, value := range rowdiff {
				sumtemp += value
			}
			Sum = append(Sum, strconv.Itoa(loadLimit-sumtemp))

		} else {
			var sumtemp int
			for _, value := range row {
				rowtemp, _ = strconv.Atoi(value)
				sumtemp += rowtemp
				rowlast = append(rowlast, rowtemp)
			}
			FileWriteToCSV = append(FileWriteToCSV, row)
			Sum = append(Sum, strconv.Itoa(loadLimit-sumtemp))
		}
		i++

	}
	FileWriteToCSV = append(FileWriteToCSV, Sum)

	// 写入csv文件
	f, err := os.Create("RemainedTxDiffMigNoErr10min64Apr12.csv") //创建文件
	if err != nil {
		panic(err)
	}
	defer f.Close()

	w := csv.NewWriter(f) //创建一个新的写入文件流
	// WriteAll方法使用Write方法向w写入多条记录，并在最后调用Flush方法清空缓存。
	w.WriteAll(FileWriteToCSV)
	w.Flush()

}

func SampleAccount(TopAccFreq map[string]int, FromAddLoc_tocsv [][]string) {
	var SampledAcc [][]string
	var SampleAccMap = make(map[string]bool)
	for i := 0; i < len(FromAddLoc_tocsv); i++ {
		if i%100 == 0 {
			SampleAccMap[FromAddLoc_tocsv[i][0]] = true
			SampledAcc = append(SampledAcc, []string{FromAddLoc_tocsv[i][0]})
		}
	}

	for key, value := range TopAccFreq {
		_, ok := SampleAccMap[key]
		if !ok {
			SampledAcc = append(SampledAcc, []string{key})
			fmt.Println(value)
		}
	}

	// 写入csv文件
	f, err := os.Create("SampledAccount70to79.csv") //创建文件
	if err != nil {
		panic(err)
	}
	defer f.Close()

	w := csv.NewWriter(f) //创建一个新的写入文件流
	// WriteAll方法使用Write方法向w写入多条记录，并在最后调用Flush方法清空缓存。
	w.WriteAll(SampledAcc)
	w.Flush()

}

func TxsForSampledAccount() {
	var SampledAccountMap = make(map[string]bool)
	var SampledTxs [][]string

	fs, err := os.Open("SampledAccount70to79.csv")
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

		SampledAccountMap[row[0]] = true
	}

	SampledTxs = ReadFileForTxsForSampledAccount("../0to999999_ERC20Transaction.csv", SampledAccountMap, SampledTxs)
	fmt.Println(len(SampledTxs))
	SampledTxs = ReadFileForTxsForSampledAccount("../1000000to1999999_ERC20Transaction.csv", SampledAccountMap, SampledTxs)
	fmt.Println(len(SampledTxs))
	SampledTxs = ReadFileForTxsForSampledAccount("../2000000to2999999_ERC20Transaction.csv", SampledAccountMap, SampledTxs)
	fmt.Println(len(SampledTxs))
	SampledTxs = ReadFileForTxsForSampledAccount("../3000000to3999999_ERC20Transaction.csv", SampledAccountMap, SampledTxs)
	fmt.Println(len(SampledTxs))
	SampledTxs = ReadFileForTxsForSampledAccount("../4000000to4999999_ERC20Transaction.csv", SampledAccountMap, SampledTxs)
	fmt.Println(len(SampledTxs))
	SampledTxs = ReadFileForTxsForSampledAccount("../5000000to5999999_ERC20Transaction.csv", SampledAccountMap, SampledTxs)
	fmt.Println(len(SampledTxs))
	SampledTxs = ReadFileForTxsForSampledAccount("../6000000to6999999_ERC20Transaction.csv", SampledAccountMap, SampledTxs)
	fmt.Println(len(SampledTxs))
	SampledTxs = ReadFileForTxsForSampledAccount("../7000000to7999999_ERC20Transaction.csv", SampledAccountMap, SampledTxs)
	fmt.Println(len(SampledTxs))
	SampledTxs = ReadFileForTxsForSampledAccount("../8000000to8999999_ERC20Transaction.csv", SampledAccountMap, SampledTxs)
	fmt.Println(len(SampledTxs))

	// 写入csv文件
	f, err := os.Create("SampledTxsApr14.csv") //创建文件
	if err != nil {
		panic(err)
	}
	defer f.Close()

	w := csv.NewWriter(f) //创建一个新的写入文件流
	// WriteAll方法使用Write方法向w写入多条记录，并在最后调用Flush方法清空缓存。
	w.WriteAll(SampledTxs)
	w.Flush()
}

func ReadFileForTxsForSampledAccount(fileName string, SampledAccountMap map[string]bool, SampledTxs [][]string) [][]string {
	i := 0

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

		_, ok := SampledAccountMap[row[4]]
		if i != 0 && ok {
			SampledTxs = append(SampledTxs, row)
		}

		i++
		if i%10000000 == 0 {
			fmt.Println(i)
		}
	}
	return SampledTxs
}

func CountTxFreqForCertainAcc() {
	i := 0
	timeslot := 3600
	var TimeCheckPoint int
	var Count [][]string
	counter := 0

	fs, err := os.Open("AccountInfoForSampled_top3rd.csv")
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

		if i == 0 {
			TimeCheckPoint, _ = strconv.Atoi(row[1])
			TimeCheckPoint += timeslot
		} else {
			timestamptemp, _ := strconv.Atoi(row[1])
			if timestamptemp <= TimeCheckPoint {

			} else {
				TimeCheckPoint += timeslot
				Count = append(Count, []string{strconv.Itoa(counter)})
				counter = 0
			}

		}

		counter++
		i++
		if i%10000000 == 0 {
			fmt.Println(i)
		}
	}

	// 写入csv文件
	f, err := os.Create("Count3rdSampledAcc.csv") //创建文件
	if err != nil {
		panic(err)
	}
	defer f.Close()

	w := csv.NewWriter(f) //创建一个新的写入文件流
	// WriteAll方法使用Write方法向w写入多条记录，并在最后调用Flush方法清空缓存。
	w.WriteAll(Count)
	w.Flush()

}

func WriteTopAcc(TopAccountFrequency map[string]int) {
	var TopAccToCSV [][]string
	for key, _ := range TopAccountFrequency {
		TopAccToCSV = append(TopAccToCSV, []string{key})
	}

	WriteAddLocToCSV(TopAccToCSV, "TopAcc30720000to31680000.csv")
}
