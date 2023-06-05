package utils

import "reflect"

// StringsContain 判断字符串数组中是否包含某个字符串
func StringsContain(strs []string, str string) bool {
	for i := range strs {
		if strs[i] == str {
			return true
		}
	}
	return false
}

func ConsumerContains(slice interface{}, obj interface{}) bool {
	s := reflect.ValueOf(slice)
	if s.Kind() != reflect.Slice {
		return false
	}

	for i := 0; i < s.Len(); i++ {
		v := s.Index(i)
		if reflect.DeepEqual(v.Interface(), obj) {
			return true
		}
	}
	return false
}

type KeyValueGetter func(key string) (string, bool)
type KeyValueIterator func(KeyValueIterateFunc)
type KeyValueIterateFunc func(key, val string) (stop bool)

const (
	LogKeyDagInsID = "dagInsId"
)
