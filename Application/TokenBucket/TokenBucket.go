package TokenBucket


import (
"time"
)

type TokenBucket struct{
	MaxSize int
	CurrentSize int
	LastReset int64
	Interval int64
}

func (tb *TokenBucket) Init(maxsize,interval int){
	tb.MaxSize = maxsize
	tb.CurrentSize = maxsize
	tb.Interval = int64(interval)
	tb.LastReset = Now()
}
func(tb *TokenBucket) UpdateTB() bool{
	if IsPast(tb.LastReset+tb.Interval){
		tb.CurrentSize = tb.MaxSize
		tb.LastReset = tb.LastReset+tb.Interval
	}
	if tb.CurrentSize <= 0{
		return false
	}
	tb.CurrentSize--
	return true
}
func Now() int64 {
	return time.Now().Unix()
}

func IsPast(stored int64) bool {
	return stored < Now()
}