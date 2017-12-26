// 封装API常用方法
package dister

import (
    "errors"
    "gitee.com/johng/gf/g/encoding/gjson"
)

// Api数据查询
func (n *Node) getDataByApi(k string) ([]byte, error) {
    if k == "" {
        if n.DataMap.Size() > 1000 {
            return nil, errors.New("too large data size, need a key to search")
        } else {
            if b, err := gjson.Encode(*n.DataMap.Clone()); err != nil {
                return nil, err
            } else {
                return b, nil
            }
        }
    } else {
        if n.DataMap.Contains(k) {
            return []byte(n.DataMap.Get(k)), nil
        } else {
            return nil, errors.New("data not found")
        }
    }
}

// Api Service查询
func (n *Node) getServiceByApi(name string) ([]byte, error) {
    if name == "" {
        if n.Service.Size() > 1000 {
            return nil, errors.New("too large service size, need a service name to search")
        } else {
            if b, err := gjson.Encode(n.getServiceMapForApi()); err != nil {
                return nil, err
            } else {
                return b, nil
            }
        }
    } else {
        if sc := n.getServiceForApiByName(name); sc != nil {
            if b, err := gjson.Encode(sc); err != nil {
                return nil, err
            } else {
                return b, nil
            }
        } else {
            return nil, errors.New("service not found")
        }
    }
}

