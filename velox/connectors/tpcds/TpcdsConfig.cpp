/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/connectors/tpcds/TpcdsConfig.h"

namespace facebook::velox::connector::tpcds {

TpcdsConfig::TpcdsConfig(
    std::shared_ptr<const config::ConfigBase> config)
    : config_(std::move(config)) {}

bool TpcdsConfig::useVarcharN(const config::ConfigBase* session) const {
  return config_->get<bool>(kUseVarcharN, false);
}

} // namespace facebook::velox::connector::tpcds