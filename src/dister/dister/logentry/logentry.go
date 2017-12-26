// dister日志模块
// 顶部索引域：随机数(14bit，固定4位正整数)数据开始位置(43bit,8TB) 数据长度(23bit,8MB)
// 底部数据域：[消息数据](变长)
package logentry

import (
    "errors"
    "gitee.com/johng/gf/g/os/gfile"
    "sync"
    "gitee.com/johng/gf/g/encoding/gbinary"
    "os"
    "gitee.com/johng/gf/g/os/gfilepool"
    "strconv"
    "strings"
    "bytes"
    "sync/atomic"
    "gitee.com/johng/gf/g/util/grand"
)

const (
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
    log := &Log {path : path}
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

// 根据id获取对应的日志文件的绝对路径
func (log *Log) getFilePathById(id uint64) string {
    fnum  := int(id/uint64(gFILE_MAX_COUNT))
    path  := log.path + gfile.Separator + strconv.Itoa(fnum)
    return path
}

// 根据消息id计算索引在日志文件中的偏移量
func (log *Log) getIndexOffsetById(id uint64) int64 {
    return int64(id%gFILE_MAX_COUNT)*gFILE_INDEX_ITEM_SIZE
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
    }
    log.maxid = maxid
}

func (log *Log) getMaxId() uint64 {
    return atomic.LoadUint64(&log.maxid)
}

func (log *Log) setMaxId(id uint64) {
    atomic.StoreUint64(&log.maxid, id)
}

// 获取日志的总数
func (log *Log) Length() uint64 {
    if log.getMaxId() > 0 {
        return log.getMaxId()/10000
    }
    return 0
}

// 添加日志
func (log *Log) Add(msg []byte) error {
    // 锁定日志文件
    log.mu.Lock()
    defer log.mu.Unlock()

    id        := log.maxid + 10000 + uint64(grand.Rand(0, 9999))
    file, err := log.getFileById(id)
    if err != nil {
        return err
    }
    ixoffset := log.getIndexOffsetById(id)

    // 日志数据写到文件末尾
    dataOffset, err := file.Seek(0, 2)
    if err != nil {
        return err
    }
    if dataOffset < gFILE_INDEX_LENGTH {
        dataOffset = gFILE_INDEX_LENGTH
        // 文件需要初始化，索引域初始化为0
        if _, err := file.WriteAt(make([]byte, gFILE_INDEX_LENGTH), 0); err != nil {
            return err
        }
    }
    if _, err := file.WriteAt(msg, dataOffset); err != nil {
        return err
    }
    // 数据写入成功后再写入索引域
    bits   := make([]gbinary.Bit, 0)
    bits    = gbinary.EncodeBits(bits, uint(dataOffset),  40)
    bits    = gbinary.EncodeBits(bits, uint(len(msg)),    24)
    indexb := append(gbinary.EncodeInt8(1), gbinary.EncodeBitsToBytes(bits)...)
    if _, err := file.WriteAt(indexb, ixoffset); err != nil {
        return err
    }
    // 执行成功之后最大id才会递增
    log.maxid = id
    return nil
}

// 根据消息id查询日志
func (log *Log) Get(id uint64) []byte {
    file, err := log.getFileById(id)
    if err != nil {
        return nil
    }
    offset := log.getIndexOffsetById(id)
    if ixbuffer := gfile.GetBinContentByTwoOffsets(file, offset, offset + gFILE_INDEX_ITEM_SIZE); ixbuffer != nil {
        status := gbinary.DecodeToInt8(ixbuffer[0 : 1])
        if status > 0 {
            bits   := gbinary.DecodeBytesToBits(ixbuffer[1 : ])
            start  := gbinary.DecodeBits(bits[0 : 40])
            size   := gbinary.DecodeBits(bits[40 : ])
            end    := start + size
            return gfile.GetBinContentByTwoOffsets(file, int64(start), int64(end))
        }
    }
    return nil
}
