package main

import (
	"encoding/csv"
	"fmt"
	"graph/test"
	"io"
	"log"
	"math/big"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"
)

// 随机变量
var r = rand.New(rand.NewSource(99))
var desiredStdDev = 100.0

var ShardNum = 4

// 为map声名并为其赋值
var FromAddLoc = make(map[string]int)

var ToAddLoc = make(map[string]int)

var Load_on_EachShard = make([]int, ShardNum)

var MigrationTime = 0

var TempFromAddLoc = make(map[string]int)

var TempToAddLoc = make(map[string]int)

var loadLimit = 120000

var MoveTx_total = 0

var RemainedTxs_total = 0

var CrossShardTx_total = 0

type Pair struct {
	Key   string
	Value int
}

type PairList []Pair

func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p PairList) Len() int           { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }

var TopAccountFrequency = make(map[string]int)

func main() {

	// 测试用函数
	// test.Test()

	// 读取address location文件
	FromAddLoc_tocsv := test.ReadAddLoc("from_address_location.csv")
	ToAddLoc_tocsv := test.ReadAddLoc("to_address_location.csv")

	// fmt.Println(FromAddLoc_tocsv[0], ToAddLoc_tocsv[1][1])

	// 以下也是一些测试
	// for i := 0; i < 10; i++ {
	// 	normrd := r.NormFloat64() * desiredStdDev

	// 	fmt.Println(int(normrd))
	// }

	// origin := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 6}
	// for i := 0; i < len(origin); i++ {
	// 	if origin[i] == 6 {
	// 		origin = append(origin[:i], origin[i+1:]...)
	// 		i-- // maintain the correct index
	// 	}
	// }

	// 变量声名
	i := 0
	var from_address []string
	var to_address []string
	var blockTimeStamp []string
	var UnixTime []int64
	loc, _ := time.LoadLocation("Local")

	var LoadEachShard_tocsv [][]string
	var RemainedTxs_tocsv [][]string

	var AccountFrequency = make(map[string]int)

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

	// 计算每个账户发送交易数量，并由多到少排序
	for _, account := range from_address {
		AccountFrequency[account]++
	}

	p := make(PairList, len(AccountFrequency))
	i = 0
	for k, v := range AccountFrequency {
		p[i] = Pair{k, v}
		i++
	}
	sort.Sort(sort.Reverse(p))

	// 得到那些发送交易top的账户
	Sum := 0
	for i := 0; i < len(p); i++ {
		Sum += p[i].Value
		TopAccountFrequency[p[i].Key] = p[i].Value
		if Sum > 410000 {
			fmt.Println(i, p[i], Sum)
			break
		}
	}

	// 声名变量
	// timeSlot := 3600 * 24
	// timeCheckPoint := UnixTime[0]

	CheckPoint := 0
	var TempFromAdd []string
	var TempToAdd []string
	var TempTime []int64
	var Whether bool
	var TotalLoadonEachShard = make([]int, ShardNum)
	var Renew_Load_on_EachShard []int
	// 定义剩余的交易（其实是发送交易的账户）
	var RemainedTxs = make([][]string, ShardNum)
	// 定义处理过的交易（其实是真正的交易）,行是交易，列是发送、接收的账户
	var ExecutedTxs [][]string

	// 根据相同时间间隔，或相同交易数量划分每个epoch
	for i := 0; i < len(UnixTime); i++ {

		// 如果没到一个epoch的长度时，不断增加交易数量
		// if UnixTime[i] < timeCheckPoint+int64(timeSlot) {

		// 如果没到loadLimit个交易，不断增加交易数量
		if CheckPoint < loadLimit {

			TempFromAdd = append(TempFromAdd, from_address[i])
			TempToAdd = append(TempToAdd, to_address[i])
			TempTime = append(TempTime, UnixTime[i])
			CheckPoint++

			// fmt.Println(UnixTime[i])

			// 如果到了一个epoch的长度，或到了loadLimit个交易，则开始处理搬移账户
		} else {

			// timeCheckPoint = UnixTime[i]

			//如果有新来的交易，初始化其放置位置（-1）
			FromAddLoc = RenewAddLoc(TempFromAdd, FromAddLoc)
			ToAddLoc = RenewAddLoc(TempToAdd, ToAddLoc)
			// fmt.Println(FromAddLoc)

			// 判断是否是Semi-full load
			Whether, Load_on_EachShard = SemiFull(Load_on_EachShard, TempFromAdd, TempToAdd, loadLimit, ShardNum, FromAddLoc, ToAddLoc, RemainedTxs)

			// 只有当semi-full的时候
			// if Whether == true {

			fmt.Println(Whether)

			// 搬移用户的结果
			Renew_Load_on_EachShard, RemainedTxs, ExecutedTxs = AccountMigration(Load_on_EachShard, FromAddLoc, ToAddLoc, TempFromAdd, TempToAdd, TempTime, RemainedTxs, ExecutedTxs)

			// 更新将要写入csv的address location
			FromAddLoc_tocsv = test.RenewAddLocToCSV(FromAddLoc_tocsv, FromAddLoc)
			ToAddLoc_tocsv = test.RenewAddLocToCSV(ToAddLoc_tocsv, ToAddLoc)

			// // 不搬移用户的结果
			// Renew_Load_on_EachShard, RemainedTxs, ExecutedTxs = NoAccountMigration(Load_on_EachShard, FromAddLoc, ToAddLoc, TempFromAdd, TempToAdd, TempTime, RemainedTxs, ExecutedTxs)

			// } else {
			// 	Renew_Load_on_EachShard = Load_on_EachShard
			// 	RemainedTxs = make([][]string, ShardNum)
			// }

			for j := 0; j < ShardNum; j++ {
				if Renew_Load_on_EachShard[j] < loadLimit/ShardNum {
					TotalLoadonEachShard[j] = TotalLoadonEachShard[j] + Renew_Load_on_EachShard[j]
				} else {
					TotalLoadonEachShard[j] = TotalLoadonEachShard[j] + loadLimit/ShardNum
				}

			}

			// CheckPoint++

			fmt.Println("finish epoch:", i/loadLimit)
			fmt.Println("total txs:", i)
			for i_remainedtxs := 0; i_remainedtxs < ShardNum; i_remainedtxs++ {
				fmt.Println("ramained txs:", len(RemainedTxs[i_remainedtxs]))
				RemainedTxs_total = RemainedTxs_total + len(RemainedTxs[i_remainedtxs])
			}

			CheckPoint = 1

			TempFromAdd = TempFromAdd[:0]
			TempToAdd = TempToAdd[:0]
			TempTime = TempTime[:0]
			TempFromAddLoc = make(map[string]int)
			TempToAddLoc = make(map[string]int)

			TempFromAdd = append(TempFromAdd, from_address[i])
			TempToAdd = append(TempToAdd, to_address[i])
			TempTime = append(TempTime, UnixTime[i])

			var Renew_Load_on_EachShard_string = make([]string, ShardNum)
			var RemainedTxs_on_EachShard_string = make([]string, ShardNum)

			// 需要写入csv文件的东西
			for i_csv := 0; i_csv < ShardNum; i_csv++ {
				Renew_Load_on_EachShard_string[i_csv] = strconv.Itoa(Renew_Load_on_EachShard[i_csv])
				RemainedTxs_on_EachShard_string[i_csv] = strconv.Itoa(len(RemainedTxs[i_csv]))
			}
			LoadEachShard_tocsv = append(LoadEachShard_tocsv, Renew_Load_on_EachShard_string)
			RemainedTxs_tocsv = append(RemainedTxs_tocsv, RemainedTxs_on_EachShard_string)

		}

	}

	// // 测试用，根据相同交易数量划分epoch
	// for i, _ := range UnixTime {
	// 	if CheckPoint == loadLimit {
	// 		fmt.Println("finish epoch ", i/loadLimit)
	// 		CheckPoint = 0
	// 		TempFromAdd = TempFromAdd[:0]
	// 		TempToAdd = TempToAdd[:0]
	// 		TempTime = TempTime[:0]
	// 		TempFromAddLoc = make(map[string]int)
	// 		// fmt.Println(TempTime)
	// 	}

	// 	if CheckPoint < loadLimit {
	// 		TempFromAdd = append(TempFromAdd, from_address[i])
	// 		TempToAdd = append(TempToAdd, to_address[i])
	// 		TempTime = append(TempTime, UnixTime[i])
	// 		CheckPoint++

	// 		if CheckPoint == loadLimit {

	// 			FromAddLoc = RenewAddLoc(TempFromAdd, FromAddLoc)
	// 			// fmt.Println(FromAddLoc)

	// 			// 判断是否是Semi-full load
	// 			Whether, Load_on_EachShard = SemiFull(Load_on_EachShard, TempFromAdd, loadLimit, ShardNum, FromAddLoc)

	// 			// 只有当semi-full的时候
	// 			if Whether == true {
	// 				fmt.Println(Whether)

	// 				// 搬移用户的结果
	// 				Renew_Load_on_EachShard = AccountMigration(Load_on_EachShard, FromAddLoc, TempFromAdd, TempTime)

	// 				// // 不搬移用户的结果
	// 				// Renew_Load_on_EachShard = Load_on_EachShard

	// 				for j := 0; j < ShardNum; j++ {
	// 					TotalLoadonEachShard[j] = TotalLoadonEachShard[j] + Renew_Load_on_EachShard[j]
	// 				}
	// 			}
	// 		}
	// 	}
	// }

	fmt.Println(TotalLoadonEachShard)

	RemainedTxsLastEpoch := 0
	for i_remainedtxs := 0; i_remainedtxs < ShardNum; i_remainedtxs++ {
		RemainedTxsLastEpoch = RemainedTxsLastEpoch + len(RemainedTxs[i_remainedtxs])
	}
	fmt.Println("total remained txs:", RemainedTxs_total, "remained txs in last epoch:", RemainedTxsLastEpoch)

	test.WriteAddLocToCSV(FromAddLoc_tocsv, "from_address_location_all_nomig.csv")
	test.WriteAddLocToCSV(ToAddLoc_tocsv, "to_address_location_nomig.csv")

	// 写入csv文件
	f, err := os.Create("remainedtx_nomig_top_witherror.csv") //创建文件
	if err != nil {
		panic(err)
	}
	defer f.Close()

	w := csv.NewWriter(f) //创建一个新的写入文件流
	// WriteAll方法使用Write方法向w写入多条记录，并在最后调用Flush方法清空缓存。
	w.WriteAll(RemainedTxs_tocsv)
	w.Flush()

	f1, err := os.Create("load_nomig_top_witherror.csv") //创建文件
	if err != nil {
		panic(err)
	}
	defer f1.Close()

	w1 := csv.NewWriter(f1) //创建一个新的写入文件流
	// WriteAll方法使用Write方法向w写入多条记录，并在最后调用Flush方法清空缓存。
	w1.WriteAll(LoadEachShard_tocsv)
	w1.Flush()

}

func RenewAddLoc(TempAdd []string, AddLoc map[string]int) map[string]int {
	for i, _ := range TempAdd {
		_, ok := AddLoc[TempAdd[i]]
		if ok {

		} else {
			AddLoc[TempAdd[i]] = -1
		}
	}

	return AddLoc
}

func SemiFull(Load_on_EachShard []int, TempFromAdd []string, TempToAdd []string, loadLimit int, ShardNum int, FromAddLoc map[string]int, ToAddLoc map[string]int, RemainedTxs [][]string) (bool, []int) {
	LoadLimit_EachShard := loadLimit / ShardNum
	Load_on_EachShard = LoadonEachShard(TempFromAdd, TempToAdd, ShardNum, FromAddLoc, ToAddLoc, RemainedTxs)

	min_load := Load_on_EachShard[0]
	max_load := Load_on_EachShard[0]
	for _, load := range Load_on_EachShard {
		if load <= min_load {
			min_load = load
		}
		if load >= max_load {
			max_load = load
		}
	}

	if max_load <= LoadLimit_EachShard {
		return false, Load_on_EachShard
	} else {
		return true, Load_on_EachShard
	}

}

// 根据用户地址计算每个shard上的load，此处要用到big包
func LoadonEachShard(TempFromAdd []string, TempToAdd []string, ShardNum int, FromAddLoc_func map[string]int, ToAddLoc_func map[string]int, RemainedTxs [][]string) []int {
	result := make([]int, ShardNum)
	for i := 0; i < ShardNum; i++ {
		result[i] = len(RemainedTxs[i])
	}

	BigShardNum := big.NewInt(int64(ShardNum))

	for _, s := range TempFromAdd {
		// 当用户是新加入用户时，根据其地址分配shard
		if FromAddLoc_func[s] == -1 {
			// v, _ := strconv.ParseInt(s, 0, 64)
			v := new(big.Int)
			v.SetString(s, 0)
			// fmt.Println(v)
			v.Mod(v, BigShardNum)
			// fmt.Println(v)
			v_int := v.Int64()
			// fmt.Println(v_int)
			FromAddLoc[s] = int(v_int)
			TempFromAddLoc[s] = int(v_int)

			for i := 0; i < ShardNum; i++ {
				if int(v_int) == i {
					result[i]++
				}
			}
			// 如果不是新用户，则按照以前的分配规则分配shard
		} else {
			for i := 0; i < ShardNum; i++ {
				if FromAddLoc_func[s] == i {
					result[i]++
					TempFromAddLoc[s] = i
					FromAddLoc[s] = i
				}
			}
		}
	}

	// 对to shard也做改变，更新对应to address应该放置的位置，但并不更新交易放置位置
	for _, s := range TempToAdd {
		// 当用户是新加入用户时，根据其地址分配shard
		if ToAddLoc_func[s] == -1 {
			// v, _ := strconv.ParseInt(s, 0, 64)
			v := new(big.Int)
			v.SetString(s, 0)
			// fmt.Println(v)
			v.Mod(v, BigShardNum)
			// fmt.Println(v)
			v_int := v.Int64()
			// fmt.Println(v_int)
			ToAddLoc[s] = int(v_int)
			TempToAddLoc[s] = int(v_int)

			// 如果不是新用户，则按照以前的分配规则分配shard
		} else {
			for i := 0; i < ShardNum; i++ {
				if ToAddLoc_func[s] == i {
					TempToAddLoc[s] = i
					ToAddLoc[s] = i
				}
			}
		}
	}

	return result
}

func AccountMigration(Load_on_EachShard []int, FromAddLoc map[string]int, ToAddLoc map[string]int, TempFromAdd []string, TempToAdd []string, TempTime []int64, RemainedTxs [][]string, ExecutedTxs [][]string) ([]int, [][]string, [][]string) {
	Counter := 0
	var MaxLoadShard int
	var max_load int
	// var TargetShard int
	var MigrationLoss int
	var NewLoadonEachShard []int
	var NewLoadonEachShard_rand = make([]int, ShardNum)
	var NewTPSloss int
	TotalMigrationLoss := 0
	var MoveTx int

	LoadSum := 0
	for i := 0; i < ShardNum; i++ {
		LoadSum = LoadSum + Load_on_EachShard[i]
	}
	// 初始化TPSloss，什么用户都还没移动时的TPSloss
	TPSloss := 0
	for _, s := range Load_on_EachShard {
		if s >= LoadSum/ShardNum {
			TPSloss = TPSloss + s - LoadSum/ShardNum
		}
	}

BreakPoint:
	MaxLoadShard = 0
	max_load = 0
	// TargetShard = 0
	// 找load最大的shard
	for i, load := range Load_on_EachShard {
		if load >= max_load {
			max_load = load
			MaxLoadShard = i
		}
	}

	// BreakPoint2:
	// 在最大load的shard上找人做migration
	for k, _ := range TempFromAddLoc {

		// 对那些top的account做搬移，其余忽略
		_, ok := TopAccountFrequency[k]
		if TempFromAddLoc[k] == MaxLoadShard && ok {

			// TODO: 这咋回事啊？
			normrd := r.NormFloat64() * float64(TopAccountFrequency[k]) * 0.3 / 150.0

			MigrationLoss = 0

			// 算出该用户在搬移期间会发生多少笔交易，这些交易是migration loss
			for index, i_time := range TempTime {
				if i_time < TempTime[0]+int64(MigrationTime) {
					if TempFromAdd[index] == k {
						MigrationLoss++
					}
				} else {
					break
				}
			}

			//预测接下来一个epoch每个shard会产生多少交易，并算出在搬移期间（约几十秒（20s,按照每个shardTPS50的话就是最多容纳1000比））会产生多少交易

			// 遍历其余所有shard，并验证搬移该用户能否降低TPS loss
			for index := 0; index < ShardNum; index++ {
				if index != MaxLoadShard {
					NewTPSloss = 0
					MoveTx = 0
					// 将该用户搬移到该shard并计算TPSloss，注意搬移用户影响到to和from
					TempFromAddLoc[k] = index
					TempToAddLoc[k] = index

					NewLoadonEachShard = LoadonEachShard(TempFromAdd, TempToAdd, ShardNum, TempFromAddLoc, TempToAddLoc, RemainedTxs)

					NewLoadonEachShard[index] = NewLoadonEachShard[index] - MigrationLoss

					// 一并将在原本shard中的该用户的remained txs转移到目标shard
					for _, i_fromAdd := range RemainedTxs[MaxLoadShard] {
						if i_fromAdd == k {
							MoveTx++
						}
					}

					NewLoadonEachShard[index] = NewLoadonEachShard[index] + MoveTx
					NewLoadonEachShard[MaxLoadShard] = NewLoadonEachShard[MaxLoadShard] - MoveTx

					// 模拟预测误差
					NewLoadonEachShard_rand[index] = NewLoadonEachShard[index] + MoveTx + int(normrd)
					NewLoadonEachShard_rand[MaxLoadShard] = NewLoadonEachShard[MaxLoadShard] - MoveTx - int(normrd)

					// 计算新的TPS loss
					for _, s := range NewLoadonEachShard_rand {
						if s >= LoadSum/ShardNum {
							NewTPSloss = NewTPSloss + s - LoadSum/ShardNum
						}
					}
					NewTPSloss = NewTPSloss + MigrationLoss

					// 如果搬移该用户能够降低TPS loss
					if NewTPSloss < TPSloss {
						// 更新全局address location
						FromAddLoc[k] = index
						ToAddLoc[k] = index

						// 更新TPSloss
						TPSloss = NewTPSloss
						// 更新总的migration loss
						TotalMigrationLoss = TotalMigrationLoss + MigrationLoss
						// 更新每个shard上的load
						Load_on_EachShard = NewLoadonEachShard
						// 更新remained txs，将原来shard上该用户的交易转移到新shard上
						// TODO: 将搬移的用户放在remainedtxs的队尾还是队首？
						for index_fromAdd := 0; index_fromAdd < len(RemainedTxs[MaxLoadShard]); index_fromAdd++ {
							if RemainedTxs[MaxLoadShard][index_fromAdd] == k {
								// RemainedTxs[index] = append(RemainedTxs[index], k)
								RemainedTxs[index] = append([]string{k}, RemainedTxs[index]...)
								RemainedTxs[MaxLoadShard] = append(RemainedTxs[MaxLoadShard][:index_fromAdd], RemainedTxs[MaxLoadShard][index_fromAdd+1:]...)
								index_fromAdd--

							}
						}

						// 计算被搬移的交易数量，包括账户搬移
						MoveTx_total = MoveTx_total + MoveTx + 1
						// 由于migration导致一些交易没了
						for deleteindex := 0; deleteindex < len(TempTime); deleteindex++ {
							if TempTime[deleteindex] < TempTime[0]+int64(MigrationTime) {
								if TempFromAdd[deleteindex] == k {
									TempFromAdd = append(TempFromAdd[:deleteindex], TempFromAdd[deleteindex+1:]...)
									TempToAdd = append(TempToAdd[:deleteindex], TempToAdd[deleteindex+1:]...)
									TempTime = append(TempTime[:deleteindex], TempTime[deleteindex+1:]...)
									deleteindex--
								}
							} else {
								break
							}
						}
						// 清零计数器
						Counter = 0

						// 重复循环
						goto BreakPoint

						// 如果搬移该用户不能降低TPS loss
					} else {
						// 用户并不进行搬移，将其放回
						TempFromAddLoc[k] = MaxLoadShard
						TempToAddLoc[k] = MaxLoadShard
						// 增加计数器
						Counter++
					}
				}
			}

			// // 可选，选择循环截止的时间
			// if Counter < 50 {
			// 	MaxLoadCompare := max_load
			// 	max_load = 0
			// 	for i, load := range Load_on_EachShard {
			// 		if load >= max_load && load < MaxLoadCompare {
			// 			max_load = load
			// 			MaxLoadShard = i
			// 		}
			// 	}

			// 	goto BreakPoint2
			// }

		}
	}

	// 更新remained txs
	for i, v := range Load_on_EachShard {
		// 如果现有的load（包括之前剩余的和新来的）大于每个shard的loadlimit
		if v > loadLimit/ShardNum {
			for index_to, s := range TempFromAdd {
				if TempFromAddLoc[s] == i {
					RemainedTxs[i] = append(RemainedTxs[i], s)
					// if len(RemainedTxs[i]) <= loadLimit/ShardNum {
					ExecutedTxs = append(ExecutedTxs, []string{TempFromAdd[index_to], TempToAdd[index_to]})
					if TempFromAddLoc[TempFromAdd[index_to]] != TempToAddLoc[TempToAdd[index_to]] {
						CrossShardTx_total++
					}
					// }
				}
			}

			RemainedTxs[i] = RemainedTxs[i][loadLimit/ShardNum:]

			// 如果现有load不大于每个shard的loadlimit，则清空remained txs
		} else {

			for index_to, s := range TempFromAdd {
				if TempFromAddLoc[s] == i {
					ExecutedTxs = append(ExecutedTxs, []string{TempFromAdd[index_to], TempToAdd[index_to]})
					if TempFromAddLoc[TempFromAdd[index_to]] != TempToAddLoc[TempToAdd[index_to]] {
						CrossShardTx_total++
					}
				}
			}

			RemainedTxs[i] = RemainedTxs[i][:0]

		}
	}

	return Load_on_EachShard, RemainedTxs, ExecutedTxs

}

func NoAccountMigration(Load_on_EachShard []int, FromAddLoc map[string]int, ToAddLoc map[string]int, TempFromAdd []string, TempToAdd []string, TempTime []int64, RemainedTxs [][]string, ExecutedTxs [][]string) ([]int, [][]string, [][]string) {

	LoadSum := 0
	for i := 0; i < ShardNum; i++ {
		LoadSum = LoadSum + Load_on_EachShard[i]
	}
	// 初始化TPSloss，什么用户都还没移动时的TPSloss
	TPSloss := 0
	for _, s := range Load_on_EachShard {
		if s >= LoadSum/ShardNum {
			TPSloss = TPSloss + s - LoadSum/ShardNum
		}
	}

	// 更新remained txs
	for i, v := range Load_on_EachShard {
		// 如果现有的load（包括之前剩余的和新来的）大于每个shard的loadlimit
		if v > loadLimit/ShardNum {
			for index_to, s := range TempFromAdd {
				if TempFromAddLoc[s] == i {
					RemainedTxs[i] = append(RemainedTxs[i], s)
					// if len(RemainedTxs[i]) <= loadLimit/ShardNum {
					ExecutedTxs = append(ExecutedTxs, []string{TempFromAdd[index_to], TempToAdd[index_to]})
					if TempFromAddLoc[TempFromAdd[index_to]] != TempToAddLoc[TempToAdd[index_to]] {
						CrossShardTx_total++
					}
					// }
				}
			}

			RemainedTxs[i] = RemainedTxs[i][loadLimit/ShardNum:]

			// 如果现有load不大于每个shard的loadlimit，则清空remained txs
		} else {

			for index_to, s := range TempFromAdd {
				if TempFromAddLoc[s] == i {
					ExecutedTxs = append(ExecutedTxs, []string{TempFromAdd[index_to], TempToAdd[index_to]})
					if TempFromAddLoc[TempFromAdd[index_to]] != TempToAddLoc[TempToAdd[index_to]] {
						CrossShardTx_total++
					}
				}
			}

			RemainedTxs[i] = RemainedTxs[i][:0]

		}
	}

	return Load_on_EachShard, RemainedTxs, ExecutedTxs

}
