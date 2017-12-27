// dister日志模块
// 顶部索引域：随机数(14bit，固定4位正整数)数据开始位置(43bit,8TB) 数据长度(23bit,8MB)
// 底部数据域：[消息数据](变长)
package logentry

import (
    "errors"
    "sync"
    "os"
    "strconv"
    "strings"
    "bytes"
    "sync/atomic"
    "gitee.com/johng/gf/g/util/grand"
    "gitee.com/johng/gf/g/os/gfile"
    "gitee.com/johng/gf/g/os/gfilepool"
    "gitee.com/johng/gf/g/encoding/gbinary"
    "gitee.com/johng/gf/g/encoding/gcompress"
    "fmt"
)

// 部分常量不能随意改变，因为与文件的数据结构设计有关联
const (
    gID_RAND_LENGTH          = 10000     // 生成ID的随机数长度(例如：10000对应4位长度)
    gFILE_MAX_COUNT          = 1000000   // 每个日志文件存储的消息条数上限(不能随便改，和数据结构设计有关系)
    gFILE_INDEX_ITEM_SIZE    = 10        // 每个日志文件的索引域大小(byte)
    gFILEPOOL_TIMEOUT        = 60        // 消息日志文件指针池过期时间
    gFILE_INDEX_LENGTH       = gFILE_MAX_COUNT*gFILE_INDEX_ITEM_SIZE // 消息日志文件索引域固定大小
)

// 消息日志管理对象
type Log struct {
    mu     sync.RWMutex
    path   string // 消息日志文件存放目录(绝对路径)
    maxid  uint64 // 日志当前最大id
    offset int64  // 文件末尾偏移量(提高文件IO性能，尽可能避免异常内容数据)
}

// 日志项
type Item struct {
    Id    uint64 // ID
    Value []byte // 内容
    start int64  // 数据域文件偏移量开始
    end   int64  // 数据域文件偏移量结束
}

// 创建消息日志管理对象
func New(path string) (*Log, error) {
    if !gfile.Exists(path) {
        if err := gfile.Mkdir(path); err != nil {
            return nil, errors.New("creating log folder failed: " + err.Error())
        }
    }
    if !gfile.IsWritable(path) || !gfile.IsReadable(path) {
        return nil, errors.New("permission denied to log folder: " + path)
    }
    log := &Log{path : path}
    log.init()
    return log, nil
}

// 根据需要写入的id获取对应的文件指针
func (log *Log) getFileById(id uint64) (*os.File, error) {
    path  := log.getFilePathById(id)
    if file, err := gfilepool.OpenWithPool(path, os.O_RDWR|os.O_CREATE, gFILEPOOL_TIMEOUT); err == nil {
        return file.File(), nil
    } else {
        return nil, err
    }
}

// 根据id计算文件编号
func (log *Log) getFileNumById(id uint64) uint64 {
    return uint64(id/gID_RAND_LENGTH/gFILE_MAX_COUNT)
}

// 根据id获取对应的日志文件的绝对路径
func (log *Log) getFilePathById(id uint64) string {
    fnum  := log.getFileNumById(id)
    path  := log.path + gfile.Separator + strconv.FormatUint(fnum, 10)
    return path
}

// 根据消息id计算索引在日志文件中的偏移量
func (log *Log) getIndexOffsetById(id uint64) int64 {
    trueid := id/gID_RAND_LENGTH
    index  := trueid%gFILE_MAX_COUNT
    offset := index*gFILE_INDEX_ITEM_SIZE
    return int64(offset)
}

// 初始化日志分类，获取当前日志的最大id
func (log *Log) init() {
    fnum  := uint64(0)
    maxid := uint64(0)
    files := gfile.ScanDir(log.path)
    // 查找最大编号的日志文件
    for _, name := range files {
        if n, err := strconv.ParseUint(strings.Split(name, ".")[0], 10, 64); err == nil {
            if n > fnum {
                fnum = n
            }
        }
    }
    // 查找当前日志文件的消息数量(最大id)
    if file, err := log.getFileById(fnum*gFILE_MAX_COUNT); err == nil {
        if ixbuffer := gfile.GetBinContentByTwoOffsets(file, 0, gFILE_INDEX_LENGTH); ixbuffer != nil {
            maxid  = fnum*gFILE_MAX_COUNT
            zerob := make([]byte, gFILE_INDEX_ITEM_SIZE)
            for i := 0; i < gFILE_INDEX_LENGTH; i += gFILE_INDEX_ITEM_SIZE {
                buffer := ixbuffer[i : i + gFILE_INDEX_ITEM_SIZE]
                if bytes.Compare(zerob, buffer) == 0 {
                    if i > 0 {
                        maxid = uint64(fnum*gFILE_MAX_COUNT) + uint64((i - gFILE_INDEX_ITEM_SIZE)/gFILE_INDEX_ITEM_SIZE)
                    }
                    if maxid != 0 {
                        // 解析索引字段，获取随机数
                        bits := gbinary.DecodeBytesToBits(buffer)
                        rand := gbinary.DecodeBits(bits[0 : 14])
                        maxid = maxid*10000 + uint64(rand)
                        break
                    }
                }
            }
        }
        // 获取文件偏移量，如果文件是首次建立，那么初始化
        log.offset, _ = file.Seek(0, 2)
        if log.offset < gFILE_INDEX_LENGTH {
            file.WriteAt(make([]byte, gFILE_INDEX_LENGTH), 0)
            log.offset = gFILE_INDEX_LENGTH
        }
    }
    log.maxid = maxid
}

// 获取日志的总数
func (log *Log) Length() uint64 {
    return atomic.LoadUint64(&log.maxid)/gID_RAND_LENGTH
}

// 添加日志
func (log *Log) Add(msg []byte) (uint64, error) {
    rand := grand.Rand(0, gID_RAND_LENGTH - 1)
    id   := log.maxid + gID_RAND_LENGTH + uint64(rand)
    if err := log.AddById(id, msg); err == nil {
        return id, nil
    } else {
        return 0, err
    }
}

// 根据指定的id添加日志
func (log *Log) AddById(id uint64, msg []byte) error {
    log.mu.Lock()
    defer log.mu.Unlock()

    // id必须递增
    if id/gID_RAND_LENGTH != (log.maxid/gID_RAND_LENGTH + 1) {
        return errors.New(fmt.Sprintf("given id [%d] not match current maxid [%d]", id, log.maxid))
    }
    file, err := log.getFileById(id)
    if err != nil {
        return err
    }
    // 数据压缩
    data   := gcompress.Zlib(msg)
    length := len(data)
    if _, err := file.WriteAt(data, log.offset); err != nil {
        return err
    }
    // 数据写入成功后再写入索引域
    bits   := make([]gbinary.Bit, 0)
    bits    = gbinary.EncodeBits(bits, uint(id%gID_RAND_LENGTH), 14)
    bits    = gbinary.EncodeBits(bits, uint(log.offset),         43)
    bits    = gbinary.EncodeBits(bits, uint(length),             23)
    if _, err := file.WriteAt(gbinary.EncodeBitsToBytes(bits), log.getIndexOffsetById(id)); err != nil {
        return err
    }
    // 执行成功之后更新成员变量
    log.maxid   = id
    log.offset += int64(length)
    return nil
}

// 根据消息id查询日志
func (log *Log) Get(id uint64) []byte {
    log.mu.RLock()
    defer log.mu.RUnlock()

    file, err := log.getFileById(id)
    if err != nil {
        return nil
    }
    offset := log.getIndexOffsetById(id)
    if ixbuffer := gfile.GetBinContentByTwoOffsets(file, offset, offset + gFILE_INDEX_ITEM_SIZE); ixbuffer != nil {
        bits   := gbinary.DecodeBytesToBits(ixbuffer)
        rand   := gbinary.DecodeBits(bits[0 : 14])
        if uint(id%gID_RAND_LENGTH) == rand {
            start  := gbinary.DecodeBits(bits[14 : 57])
            size   := gbinary.DecodeBits(bits[57 : 80])
            end    := start + size
            return gfile.GetBinContentByTwoOffsets(file, int64(start), int64(end))
        }
    }
    return nil
}

// 获取比id在length长度范围内的日志列表，length > 0表示往后获取，length < 0表示往前获取
func (log *Log) GetByRange(id uint64, length int) []Item {
    log.mu.RLock()
    defer log.mu.RUnlock()
    rangeStart := uint64(0)
    rangeEnd   := uint64(0)
    // 计算范围
    if length < 0 {
        rangeStart = uint64(int(id/gID_RAND_LENGTH) + length)
        if rangeStart < 0 {
            rangeStart = 0
        }
        rangeEnd = id
    } else {
        rangeStart = id
        rangeEnd   = id + uint64(length)*gID_RAND_LENGTH
    }
    // 检索数据
    list := make([]Item, 0)
    for {
        file, err := log.getFileById(id)
        if err != nil {
            break
        }
        group  := id/gID_RAND_LENGTH
        offset := log.getIndexOffsetById(id)
        if ixbuffer := gfile.GetBinContentByTwoOffsets(file, offset, gFILE_INDEX_LENGTH); ixbuffer != nil {
            items := make([]Item, 0)
            dataOffsetStart := int64(0)
            dataOffsetEnd   := int64(0)
            for i := 0; i <= gFILE_INDEX_LENGTH - int(offset); i += gFILE_INDEX_ITEM_SIZE {
                bits   := gbinary.DecodeBytesToBits(ixbuffer[i : i + gFILE_INDEX_ITEM_SIZE])
                rand   := gbinary.DecodeBits(bits[0 : 14])
                start  := gbinary.DecodeBits(bits[14 : 57])
                size   := gbinary.DecodeBits(bits[57 : 80])
                end    := start + size
                if dataOffsetEnd == 0 {
                    dataOffsetStart = int64(start)
                }
                dataOffsetEnd = int64(end)
                items = append(items, Item {
                    Id    : group*gID_RAND_LENGTH + rand,
                    start : start,
                    end   : end,
                })

            }

        }
    }
    return list
}



// 验证所给的日志id是否合法
func (log *Log) Valid(id uint64) bool {
    file, err := log.getFileById(id)
    if err != nil {
        return false
    }
    offset := log.getIndexOffsetById(id)
    if ixbuffer := gfile.GetBinContentByTwoOffsets(file, offset, offset + gFILE_INDEX_ITEM_SIZE); ixbuffer != nil {
        if bytes.Compare(make([]byte, gFILE_INDEX_ITEM_SIZE), ixbuffer) == 0 {
            return false
        } else {
            bits := gbinary.DecodeBytesToBits(ixbuffer)
            rand := gbinary.DecodeBits(bits[0 : 14])
            if uint(id%gID_RAND_LENGTH) == rand {
                return true
            } else {
                return false
            }
        }
    }
    return false
}
