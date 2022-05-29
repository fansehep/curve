/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: 2022-05-26
 * Author: YangFan (fansehep)
 */
#ifndef SRC_MDS_NAMESERVER2_WRITER_LOCK_H
#define SRC_MDS_NAMESERVER2_WRITER_LOCK_H

#include <string>
#include <memory>

#include "src/common/encode.h"
#include "src/common/uncopyable.h"
#include "src/common/concurrent/name_lock.h"
#include "src/mds/topology/topology_storge_etcd.h"


namespace curve {
namespace mds {


using ::curve::common::NameLock;
using ::curve::common::Uncopyable;
using ::curve::kvstorage::EtcdClientImp;

struct Writer_LockOptions {
  //* writer 与 mds 会话的心跳时间 ms
  //* 默认是 5s 内 发送 4 次
  uint64_t WriterInterval = 1250;
  //* writer 可以超时的时间，超过这段时间则认为 writer 挂掉了
  uint64_t WriterTimeout = 3000;
};

//* 
enum class Permission {
  Reader,
  Writer,
};

enum class WRITER_STATUS {
  SUCCESS,
  ERROR,
  NOT_EXIST,
  EXPIRED,
};

//* 我们这里把文件的读写权限的获取抽象成一把锁，
//* 由于同时只能有一个writer, 所以同一时间内只能有一个client持有该锁
//* 
//* writer = IP(std::string) + Port(uint32_t) + date(uint64_t)
class Writer_Lock {
 public:
    Writer_Lock(const Writer_LockOptions& options, 
                const std::shared_ptr<EtcdClientImp>& etcdclient);
    ~Writer_Lock() = default;
    //* the Writer is the Client IP + Port
    //* 客户端尝试获得锁，该函数返回权限
    Permission Lock(const std::string& filename, 
                    const std::string& clientip,
                    uint32_t clientport,
                    uint64_t date);
    
    WRITER_STATUS Unlock(const std::string& filename, 
                        const std::string& clientip,
                        uint32_t clientport);
    bool UpdateWriterLock(const std::string& filename, 
                                 const std::string& ClientIp,
                                 uint32_t ClientPort,
                                 uint64_t Date);
                          
 private:
    size_t Hash(const std::string& key);

    std::string EncodeKey(const std::string& writerinfo);

    WRITER_STATUS GetWriter(const std::string& filename,
                            std::string* writer);

    bool SetWriter(const std::string& filename, 
                   const std::string& Clientip,
                   uint32_t Clientport,
                   uint64_t Date);

    bool ClearWriter(const std::string& filename);
 private:    
    NameLock namelock_;
    Writer_LockOptions w_lockoptions_;
    std::shared_ptr<EtcdClientImp> etcdclient_;
};


} //* namespace mds
} //* namespace curve

#endif