package utils

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

func DirSize(dirPath string) (int64, error) {
	var totalSize int64 = 0
	err := filepath.Walk(dirPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			totalSize += info.Size()
		}

		return nil
	})

	return totalSize, err
}

func AvailableDiskSize() (uint64, error) {
	wd, err := os.Getwd()
	if err != nil {
		return 0, err
	}

	var stat syscall.Statfs_t
	if err = syscall.Statfs(wd, &stat); err != nil {
		return 0, err
	}

	return stat.Bavail * uint64(stat.Bsize), nil
}

func CopyDir(src, dest string, exclude []string) error {
	// 判断目标目录是否存在，不存在则创建
	if _, err := os.Stat(dest); os.IsNotExist(err) {
		if err := os.MkdirAll(dest, os.ModePerm); err != nil {
			return nil
		}
	}

	// 遍历源文件进行拷贝
	return filepath.Walk(src, func(path string, info fs.FileInfo, err error) error {
		// 这里相当于把得到目标src中的文件布局
		// 比如src = "/root/bitcask"
		// 当前遍历到 = "/root/bitcask/xxx"
		// 替换后就得到xxx, n = 1表示只匹配一次
		fileName := strings.Replace(path, src, "", 1)


		if fileName == "" {
			return nil
		}

		// 查看当前文件是否在exclude列表中
		for _, ex := range exclude {
			matched, err := filepath.Match(ex, info.Name())
			if err != nil {
				return err
			}

			if matched {
				return nil
			}
		}

		// 如果是目录，则创建
		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dest, fileName), info.Mode())
		}

		// 如果是文件，则拷贝出来写入
		data, err := os.ReadFile(filepath.Join(src, fileName))
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dest, fileName), data, info.Mode())
	})
}
