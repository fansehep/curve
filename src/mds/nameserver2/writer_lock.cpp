/*
 *  Copyright (c) 2022 NetEase Inc.
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
 * Project: Curve
 * Created Date: 2022-05-26
 * Author: YangFan (fansehep)
 */

#include "writer_lock.h"
#include <bthread/bthread.h>
#include <ostream>
#include <functional>
#include "proto/nameserver2.pb.h"
#include "src/common/encode.h"
#include "src/common/timeutility.h"


namespace curve {
namespace mds {


using ::curve::common::TimeUtility;
using ::curve::common::NameLockGuard;
using ::curve::common::EncodeBigEndian;

const std::string WRITER_LOCK_PREFIX = "Writer_Lock_";
const uint64_t WRITER_LOCK_PREFIX_SIZE = 12;

inline size_t Writer_Lock::Hash(const std::string& key) {
  return std::hash<std::string>{}(key);
}

std::string Writer_Lock::EncodeKey(const std::string& writerinfo) {
  std::string key = WRITER_LOCK_PREFIX;
  auto prefix_len = WRITER_LOCK_PREFIX_SIZE;
  uint64_t num = static_cast<uint64_t>(Hash(writerinfo));
  key.resize(prefix_len + sizeof(uint64_t));
  EncodeBigEndian(&(key[prefix_len]), num);
  return key;
}

WRITER_STATUS Writer_Lock::GetWriter(const std::string& filename, \
                                     std::string* writer) {
    std::string key = EncodeKey(filename);
    std::string value; //* IP + Port
    int re = etcdclient_->Get(key, &value);
    WriterInfo writerinfo;
    if (re == EtcdErrCode::EtcdKeyNotExist) {
      return WRITER_STATUS::NOT_EXIST;
    } else if (re != EtcdErrCode::EtcdOK) {
      LOG(ERROR) << "get the value error filename = " << filename;
      return WRITER_STATUS::ERROR;
    } else if (!writerinfo.ParseFromString(value)) {
      LOG(ERROR) << "parse fail";
      return WRITER_STATUS::ERROR;
    } //GetTimeofDayMs
    // now unit is sec
    // 
    uint64_t now = TimeUtility::GetTimeofDaySec();
    //* 当现在时间 >= writer 上次的最后一次收到消息的时间 + session 间隔时间 + 允许超时的最大时间 
    if ((now - writerinfo.lastreceivetime()) > (( w_lockoptions_.WriterInterval / 1000) \
      + (w_lockoptions_.WriterTimeout / 1000))) {
        return WRITER_STATUS::EXPIRED;
    }
    std::string t_writer = writerinfo.clientip();
    char buf[64];
    snprintf(buf, sizeof(buf), " %u", writerinfo.clientport());
    t_writer += buf;
    *writer = t_writer;
    return WRITER_STATUS::SUCCESS;
}

bool Writer_Lock::SetWriter(const std::string& filename, 
                            const std::string& Clientip,
                            uint32_t Clientport,
                            uint64_t Date) {
    std::string key = EncodeKey(filename);
    std::string value;
    WriterInfo writerinfo;
    writerinfo.set_clientip(Clientip);
    writerinfo.set_clientport(Clientport);
    writerinfo.set_lastreceivetime(Date);
    if (!writerinfo.SerializeToString(&value)) {
      LOG(ERROR) << "Serialize value to string failed!";
      return false;
    }
    NameLockGuard lg(namelock_, filename);
    int rc = etcdclient_->Put(key, value);
    if (rc != EtcdErrCode::EtcdOK) {
      LOG(ERROR) << "put the key, value" << key << value << "error";
      return false;
    }
    return true;
}

bool Writer_Lock::ClearWriter(const std::string& filename) {
  std::string key = EncodeKey(filename);
  int rc = etcdclient_->Delete(key);
  if (rc != EtcdErrCode::EtcdOK) {
    LOG(ERROR) << "Delete filelock from etcd failed rc = " << rc;
    return false; 
  }
  return true;
}



Writer_Lock::Writer_Lock(const Writer_LockOptions& options, 
                         const std::shared_ptr<EtcdClientImp>& etcdclient)
                         : w_lockoptions_(options),
                           etcdclient_(etcdclient) {}

Permission Writer_Lock::Lock(const std::string& filename,
                             const std::string& clientip,
                             uint32_t clientport,
                             uint64_t date) {
    NameLockGuard lg(namelock_, filename);
    char buf[64];
    snprintf(buf, sizeof(buf), " %u", clientport);
    std::string t_writer = (clientip + buf);
    std::string writer;
    auto status = GetWriter(filename, &writer);
    //* 0) 此时，对应的文件无writer挂载
    if (status == WRITER_STATUS::NOT_EXIST) {
      LOG(INFO) << filename << " " << clientip << clientport << "has be writer";
      return Permission::Writer;
    }
    if (status == WRITER_STATUS::SUCCESS) {
      //* 1) 挂载成功了，但是 mds rpc 返回失败了，
      //*  客户端重试了，这里也可以成功挂载
      if (t_writer == writer) {
        LOG(INFO) << filename << " " << clientip << clientport << "retry open";
        return Permission::Writer;
      } else {
        //* 不同则返回 reader 权限
        LOG(INFO) << filename << " " << clientip << clientport << "is reader";
        return Permission::Reader;
      }
      //* 2) 当前的文件对应的writer超时了， 可以挂载
    } else if (status == WRITER_STATUS::EXPIRED) {
      LOG(INFO) << filename << " " << clientip << clientport << "is new writer";
      SetWriter(filename, clientip, clientport, date);
      return Permission::Writer;
    } else if (status == WRITER_STATUS::ERROR) {
      LOG(ERROR) << filename << " " << clientip << clientport << "fail get fileinfo";
      //* 3) 任何有错误都是 读权限
      return Permission::Reader;
    }
    return Permission::Reader;
}

WRITER_STATUS Writer_Lock::Unlock(const std::string& filename, 
                                const std::string& clientip,
                                uint32_t clientport) {
    NameLockGuard lg(namelock_, filename);
    char buf[64];
    snprintf(buf, sizeof(buf), " %u", clientport);
    std::string cur_writer = clientip + buf;
    std::string old_writer;
    auto status = GetWriter(filename, &old_writer);
    if( status == WRITER_STATUS::EXPIRED ||
        status == WRITER_STATUS::NOT_EXIST ||
        (status == WRITER_STATUS::SUCCESS && old_writer == cur_writer)) {
      ClearWriter(filename);
      return WRITER_STATUS::SUCCESS;
    }
    return WRITER_STATUS::ERROR;
}
//* 由于 rpc 与 mds session 的会话心跳是按顺序的
//* 这里应该不用上锁
//* 这里可以在 refreshsession 这里加入标记，
//* 只有 client 端标记为 writer,
//* 才会去调用该函数
bool Writer_Lock::UpdateWriterLock(const std::string& filename,
                                   const std::string& ClientIp,
                                   uint32_t ClientPort,
                                   uint64_t Date) {
    NameLockGuard lg(namelock_, filename);
    char buf[64];
    snprintf(buf, sizeof(buf), " %u", ClientPort);
    std::string cur_writer = ClientIp + buf;
    std::string old_writer;
    auto status = GetWriter(filename, &old_writer);
    if(status == WRITER_STATUS::ERROR) {
      LOG(ERROR) << "can not get from the etcd" << filename;
      return false;
    }
    //* RefreshSession 加入 isWriter() 可有效减少调用次数
    //* 只有确实是当前对应的文件的 writer
    //* 才更新 writer
    if(cur_writer == old_writer) {
      auto re = SetWriter(filename, ClientIp, ClientPort, Date);
      if (re == false) {
        LOG(ERROR) << "the file change writer faild";
        return false;
      } else {
        LOG(INFO) << "the file " << filename << " change writer " << cur_writer;
        return true;
      }
    }
    return false;
}




} //* namespace mds
} //* namespace curve