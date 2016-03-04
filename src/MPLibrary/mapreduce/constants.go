package mapreduce

// import (
// 	"fmt"
// 	"strconv"
// )


const(
	mapJob  	string ="map"
	reduceJob	string ="reduce"
)

// KeyValue is a type used to hold the key/value pairs passed to the map and
// reduce functions.
type KeyValue struct {
	Key   string
	Value string
}