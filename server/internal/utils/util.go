package utils

// SliceDiff 返回两个差集：
// - onlyInA: 在 slice1 中有，但在 slice2 中没有的元素
// - onlyInB: 在 slice2 中有，但在 slice1 中没有的元素
// 使用泛型 T 和自定义比较函数 equal
func SliceDiff[T any](slice1, slice2 []T, equal func(T, T) bool) (onlyInA, onlyInB []T) {
	// 计算 slice1 相对于 slice2 的差集 (onlyInA)
	for _, v1 := range slice1 {
		found := false
		for _, v2 := range slice2 {
			if equal(v1, v2) {
				found = true
				break
			}
		}
		if !found {
			onlyInA = append(onlyInA, v1)
		}
	}

	// 计算 slice2 相对于 slice1 的差集 (onlyInB)
	for _, v2 := range slice2 {
		found := false
		for _, v1 := range slice1 {
			if equal(v1, v2) {
				found = true
				break
			}
		}
		if !found {
			onlyInB = append(onlyInB, v2)
		}
	}

	return onlyInA, onlyInB // Go 支持多返回值
}
