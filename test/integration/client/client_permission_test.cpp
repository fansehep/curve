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
 * File Created: 2022-05-29 13:08
 * Author: YangFan (fansehep)
 */

#include <gtest/gtest.h>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include <map>
#include <cmath>
#include <mutex>   // NOLINT
#include <thread>  // NOLINT
#include <atomic>
#include <unordered_map>
#include <memory>
#include <string>
#include <unistd.h>
#include <numeric>
#include <algorithm>
#include <condition_variable>  // NOLINT

#include "include/client/libcurve.h"
#include "src/common/timeutility.h"
#include "src/client/client_metric.h"
#include "src/client/inflight_controller.h"
#include "test/integration/client/common/file_operation.h"
#include "test/integration/cluster_common/cluster.h"
#include "test/util/config_generator.h"

using curve::CurveCluster;


const char* kMdsConfPath = "./test/integration/unstable_test_mds.conf";
const char* kCSConfPath = "./test/integration/unstable_test_cs.conf";
const char* kClientConfPath = "./test/integration/unstable_test_client.conf";

const char* kEtcdClientIpPort = "127.0.0.1:30000";
const char* kEtcdPeerIpPort = "127.0.0.1:29999";
const char* kMdsIpPort = "127.0.0.1:30010";
const char* kClientInflightNum = "6";
const char* kLogPath = "./runlog/";

curve::client::PerSecondMetric iops("test", "iops");

std::atomic<bool> running{ false };

const std::vector<std::string> chunkserverConfigOpts{
    "chunkfilepool.enable_get_chunk_from_pool=false",
    "walfilepool.enable_get_segment_from_pool=false"
};

const std::vector<std::string> mdsConfigOpts{
    std::string("mds.etcd.endpoint=") + std::string(kEtcdClientIpPort)
};

const std::vector<std::string> clientConfigOpts{
    std::string("mds.listen.addr=") + kMdsIpPort,
    std::string("maxInFlightRPCNum=") + kClientInflightNum,
    std::string("global.logPath=") + kLogPath,
    std::string("isolation.taskQueueCapacity=128"),
    std::string("schedule.queueCapacity=128"),
};

const std::vector<std::string> mdsConf{
    std::string(" --confPath=") + kMdsConfPath,
    std::string(" --mdsAddr=") + kMdsIpPort,
    std::string(" --etcdAddr=") + kEtcdClientIpPort,
    { " --log_dir=./runlog/mds" },
    { " --stderrthreshold=3" }
};

const std::vector<std::string> chunkserverConfTemplate{
    { " -raft_sync_segments=true" },
    std::string(" -conf=") + kCSConfPath,
    { " -chunkServerPort=%d" },
    { " -chunkServerStoreUri=local://./ttt/%d/" },
    { " -chunkServerMetaUri=local://./ttt/%d/chunkserver.dat" },
    { " -copySetUri=local://./ttt/%d/copysets" },
    { " -raftSnapshotUri=curve://./ttt/%d/copysets" },
    { " -raftLogUri=curve://./ttt/%d/copysets" },
    { " -recycleUri=local://./ttt/%d/recycler" },
    { " -chunkFilePoolDir=./ttt/%d/chunkfilepool/" },
    { " -chunkFilePoolMetaPath=./ttt/%d/chunkfilepool.meta" },
    { " -walFilePoolDir=./ttt/%d/walfilepool/" },
    { " -walFilePoolMetaPath=./ttt/%d/walfilepool.meta" },
    { " -mdsListenAddr=127.0.0.1:30010,127.0.0.1:30011,127.0.0.1:30012" },
    { " -log_dir=./runlog/cs_%d" },
    { " --stderrthreshold=3" }
};

const std::vector<int> chunkserverPorts{
    31000, 31001, 31010, 31011, 31020, 31021,
};

std::vector<std::string> GenChunkserverConf(int port) {
    std::vector<std::string> conf(chunkserverConfTemplate);
    char temp[NAME_MAX_SIZE];

    auto formatter = [&](const std::string& format, int port) {
        snprintf(temp, sizeof(temp), format.c_str(), port);
        return temp;
    };

    conf[2] = formatter(chunkserverConfTemplate[2], port);
    conf[3] = formatter(chunkserverConfTemplate[3], port);
    conf[4] = formatter(chunkserverConfTemplate[4], port);
    conf[5] = formatter(chunkserverConfTemplate[5], port);
    conf[6] = formatter(chunkserverConfTemplate[6], port);
    conf[7] = formatter(chunkserverConfTemplate[7], port);
    conf[8] = formatter(chunkserverConfTemplate[8], port);
    conf[9] = formatter(chunkserverConfTemplate[9], port);
    conf[10] = formatter(chunkserverConfTemplate[10], port);
    conf[11] = formatter(chunkserverConfTemplate[11], port);
    conf[12] = formatter(chunkserverConfTemplate[12], port);
    conf[14] = formatter(chunkserverConfTemplate[14], port);

    std::string rmcmd = "rm -rf ./runlog/cs_" + std::to_string(port);
    std::string mkcmd = "mkdir -p ./runlog/cs_" + std::to_string(port);
    system(rmcmd.c_str());
    system(mkcmd.c_str());

    return conf;
}

off_t RandomWriteOffset() {
    return rand() % 32 * (16 * 1024 * 1024);
}

size_t RandomWriteLength() {
    return rand() % 32 * 4096;
}

static char buffer[1024 * 4096];

struct ChunkserverParam {
    int id;
    int port;
    std::string addr{ "127.0.0.1:" };
    std::vector<std::string> conf;

    ChunkserverParam(int id, int port) {
        this->id = id;
        this->port = port;
        this->addr.append(std::to_string(port));
        this->conf = GenChunkserverConf(port);
    }
};

class UnstableCSModuleException : public ::testing::Test {
 protected:
    static void SetUpTestCase() {
        // 清理文件夹
        system("rm -rf module_exception_curve_unstable_cs.etcd");
        system("rm -rf ttt");
        system("mkdir -p ttt");
        system("mkdir -p runlog");
        system("mkdir -p runlog/mds");

        cluster.reset(new CurveCluster());
        ASSERT_NE(nullptr, cluster.get());

        // 生成配置文件
        cluster->PrepareConfig<curve::MDSConfigGenerator>(kMdsConfPath,
                                                          mdsConfigOpts);
        cluster->PrepareConfig<curve::CSConfigGenerator>(kCSConfPath,
                                                         chunkserverConfigOpts);
        cluster->PrepareConfig<curve::ClientConfigGenerator>(kClientConfPath,
                                                             clientConfigOpts);

        // 1. 启动etcd
        pid_t pid = cluster->StartSingleEtcd(
            1, kEtcdClientIpPort, kEtcdPeerIpPort,
            std::vector<std::string>{
                " --name module_exception_curve_unstable_cs" });
        LOG(INFO) << "etcd 1 started on " << kEtcdClientIpPort << ":"
                  << kEtcdPeerIpPort << ", pid = " << pid;
        ASSERT_GT(pid, 0);

        // 2. 启动一个mds
        pid = cluster->StartSingleMDS(1, kMdsIpPort, 30013, mdsConf, true);
        LOG(INFO) << "mds 1 started on " << kMdsIpPort << ", pid = " << pid;
        ASSERT_GT(pid, 0);
        std::this_thread::sleep_for(std::chrono::seconds(2));

        // 3. 创建物理池
        ASSERT_EQ(
            0,
            cluster->PreparePhysicalPool(
                1,
                "./test/integration/client/config/unstable/"
                "topo_unstable.json"));

        // 4. 创建chunkserver
        StartAllChunkserver();
        std::this_thread::sleep_for(std::chrono::seconds(5));

        // 5. 创建逻辑池，并睡眠一段时间让底层copyset先选主
        ASSERT_EQ(0, cluster->PrepareLogicalPool(
            1, "test/integration/client/config/unstable/topo_unstable.json"));
        std::this_thread::sleep_for(std::chrono::seconds(10));

        // 6. 初始化client配置
        int ret = Init(kClientConfPath);
        ASSERT_EQ(ret, 0);

        // 7. 先睡眠10s，让chunkserver选出leader
        std::this_thread::sleep_for(std::chrono::seconds(10));
    }

    static void TearDownTestCase() {
        UnInit();
        ASSERT_EQ(0, cluster->StopCluster());
        // 清理文件夹
        system("rm -rf module_exception_curve_unstable_cs.etcd");
        system("rm -rf module_exception_curve_unstable_cs");
        system("rm -rf ttt");
    }

    static void StartAllChunkserver() {
        int id = 1;
        for (auto port : chunkserverPorts) {
            ChunkserverParam param(id, port);
            chunkServers.emplace(id, param);

            pid_t pid =
                cluster->StartSingleChunkServer(id, param.addr, param.conf);
            LOG(INFO) << "chunkserver " << id << " started on " << param.addr
                      << ", pid = " << pid;
            ASSERT_GT(pid, 0);
            std::this_thread::sleep_for(std::chrono::seconds(1));
            ++id;
        }
    }

    static void OpenAndWrite(const std::string& filename) {
        int fd = curve::test::FileCommonOperation::Open(filename, "curve");
        ASSERT_NE(-1, fd);

        std::vector<std::thread> writeThs;
        for (int i = 0; i < 5; ++i) {
            writeThs.emplace_back(AioWriteFunc, fd);
            LOG(INFO) << "write " << filename << ", thread " << (i + 1)
                      << " started";
        }

        for (auto& th : writeThs) {
            th.join();
        }

        curve::test::FileCommonOperation::Close(fd);
        LOG(INFO) << "stop all write thread, filename " << filename;
    }

    static void AioWriteFunc(int fd) {
        auto cb = [](CurveAioContext* ctx) {
            iops.count << 1;
            delete ctx;
        };

        while (running) {
            CurveAioContext* context = new CurveAioContext;
            context->op = LIBCURVE_OP::LIBCURVE_OP_WRITE;
            context->cb = cb;
            context->offset = RandomWriteOffset();
            context->length = RandomWriteLength();
            context->buf = buffer;

            AioWrite(fd, context);
        }
    }

    void SetUp() override {}

    void TearDown() override {}

    static int fd;
    static std::unique_ptr<CurveCluster> cluster;
    static std::unordered_map<int, ChunkserverParam> chunkServers;
};

int UnstableCSModuleException::fd = 0;
std::unique_ptr<CurveCluster> UnstableCSModuleException::cluster;
std::unordered_map<int, ChunkserverParam> UnstableCSModuleException::chunkServers; // NOLINT


/*
* 当前已经有了一个 writer, 和多个reader, writer 主动 close,  
* 在重新开一个 client, 测试该 client 是否可以正常 open, 并成为 writer
*/
TEST_F(UnstableCSModuleException, TestReadyWriterSomeReader) {
    const std::string filename = "/TestCommonReadAndWrite";
    constexpr size_t length = 4ull * 1024 * 1024;
    constexpr off_t offset = 4ull * 1024 * 1024;
    std::unique_ptr<char[]> readBuff(new char[length]);

    C_UserInfo_t info;

    snprintf(info.owner, sizeof(info.owner), "curve");
    info.password[0] = 0;

    ::Create(filename.c_str(), &info, 10ull * 1024 * 1024 * 1024);
    OpenFlags openflags;
    openflags.SetWantToBeWriter(true);
	int fd = ::Open2(filename.c_str(), &info, openflags);
    int ret = ::Read(fd, readBuff.get(), offset, length);
    ret = ::Write(fd, readBuff.get(), offset, length);
    ASSERT_GT(ret, 0);
	//* 此时base process 所在的进程成为 Writer
	//* fork 2 个进程，再去挂载
	auto rf = fork();
	if (rf < 0) {
		LOG(ERROR) << "fork error! please check your machine";
        exit(-1);
    }
    //* child process
	if (rf == 0) {
		int fd_c2 = ::Open(filename.c_str(), &info);
		int wr_v = ::Write(fd_c2, readBuff.get(), offset, length);
        ASSERT_EQ(-1, wr_v);
        
        auto rf_2 = fork();
        //* child process 2
        if(rf_2 == 0) {
            int fd_c3 = ::Open(filename.c_str(), &info);
            int wr_v2 = ::Write(fd_c3, readBuff.get(), offset, length);
            ASSERT_EQ(-1, wr_v2);
        }
        if(rf_2 > 0) {
            wait(&rf_2);
        }
    }
    LOG(INFO) << "Now the writer and 2 readers is success!";
    if (rf > 0) {
        ::Close(fd);
        auto rf_3 = fork();
        //* child process 3
        if (rf_3 == 0) {
            int fd_c4 = ::Open2(filename.c_str(), &info, openflags);
            int wr_v4 = ::Write(fd_c4, readBuff.get(), offset, length);
            ASSERT_GT(wr_v4, 0);
            ::Close(fd_c4);
        }
        if(rf_3 > 0) {
            wait(&rf_3);
        }
    }
    if (rf > 0) {
        wait(&rf);
    }
}

/*
* 多个client 并发去open, 验证是否只有一个client拿到writer
*
*/

TEST_F(UnstableCSModuleException, TestReadyWriterSomeReader_WithOutFakeWriter) {
    const std::string filename = "/TestCommonReadAndWrite";
    constexpr size_t length = 4ull * 1024 * 1024;
    constexpr off_t offset = 4ull * 1024 * 1024;
    std::unique_ptr<char[]> readBuff(new char[length]);

    C_UserInfo_t info;
    snprintf(info.owner, sizeof(info.owner), "curve");
    info.password[0] = 0;
    ::Create(filename.c_str(), &info, 10ull * 1024 * 1024 * 1024);
    std::vector<std::thread> workers;
    int jobs[5] = {0};
    OpenFlags openflags;
    openflags.SetWantToBeWriter(true);
    for(int i = 0; i < 5; i++) {
        workers.emplace_back([&](){
            auto rf = fork();
            if (rf == 0) {
               auto fd = ::Open2(filename.c_str(), &info, openflags);
               auto rv = ::Write(fd, readBuff.get(), offset, length);
               jobs[i] = rv;
               if(rv > 0) {
                   ::Close(fd);
               }
            }
            if (rf > 0) {
                wait(&rf);
            }
        });
    }
    for(int i = 0; i < 5; i++) {
        workers[i].join();
    }
    auto sum = 0;
    for(auto i = 0; i < 5; ++i) {
        if (jobs[i] > 0) {
            sum++;
            if(sum > 1) {
                LOG(ERROR) << "Multi Open Faild!";
            }
        } 
    }
    ASSERT_EQ(1, sum);
}

/*
*  在 2 的基础上，加上 fakewriter
*/
TEST_F(UnstableCSModuleException, TestReadyWriterSomeReader_AddFakeWriter) {
    const std::string filename = "/TestCommonReadAndWrite";
    constexpr size_t length = 4ull * 1024 * 1024;
    constexpr off_t offset = 4ull * 1024 * 1024;
    std::unique_ptr<char[]> readBuff(new char[length]);

    C_UserInfo_t info;
    snprintf(info.owner, sizeof(info.owner), "curve");
    info.password[0] = 0;
    ::Create(filename.c_str(), &info, 10ull * 1024 * 1024 * 1024);
    std::vector<std::thread> workers;
    int jobs[5] = {0};
    OpenFlags openflags;
    openflags.SetWantToBeWriter(true);
    OpenFlags fakewriter_flag;
    fakewriter_flag.SetFakeWriter(true);
    for(int i = 0; i < 5; i++) {
        workers.emplace_back([&](){
            auto rf = fork();
            if (i == 4) {
                if (rf == 0) {
                    auto fd = ::Open2(filename.c_str(), &info, fakewriter_flag);
                    auto rv = ::Write(fd, readBuff.get(), offset, length);
                    if (rv > 0) {
                        jobs[i] = -1000;
                    }
                }
                return;
            }
            if (rf == 0) {
               auto fd = ::Open2(filename.c_str(), &info, openflags);
               auto rv = ::Write(fd, readBuff.get(), offset, length);
               jobs[i] = rv;
               if(rv > 0) {
                   ::Close(fd);
               }
            }
            if (rf > 0) {
                wait(&rf);
            }
        });
    }
    auto sum = 0;
    bool fakewriter_test = false;
    for(auto i = 0; i < 5; ++i) {
        if (jobs[i] > 0) {
            sum++;
            if(sum > 1) {
                LOG(ERROR) << "Multi Open Faild!";
            }
        } else if (jobs[i] == -1000) {
            fakewriter_test = true;
        } 
    }
    ASSERT_TRUE(fakewriter_test);
    LOG(INFO) << "fake writer success open!";
    ASSERT_EQ(1, sum);
    LOG(INFO) << "success!";
}
