/*
 *  Copyright (c) 2021 NetEase Inc.
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
 * Date: Thursday Dec 09 21:18:37 CST 2021
 * Author: wuhanqing
 */

#include "src/client/client_common.h"

#include <mutex>

namespace curve {
namespace client {

OpenFlags DefaultReadonlyOpenFlags() {
    static OpenFlags readonlyFlags;
    static std::once_flag onceFlag;

    // std::call_once(onceFlag, []() { readonlyFlags.IsExeclusive() = false; });
    std::call_once(onceFlag, [](){ readonlyFlags.SetExeclusive(false);});
    return readonlyFlags;
}

}  // namespace client
}  // namespace curve
