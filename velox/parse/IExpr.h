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
#pragma once

#include <fmt/format.h>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::core {

class IExpr;
using ExprPtr = std::shared_ptr<const IExpr>;

/// An implicitly-typed expression, such as function call, literal, etc.
class IExpr {
 public:
  explicit IExpr(
      std::vector<ExprPtr> inputs,
      std::optional<std::string> alias = std::nullopt)
      : inputs_{std::move(inputs)}, alias_{std::move(alias)} {}

  virtual ~IExpr() = default;

  const std::vector<ExprPtr>& inputs() const {
    return inputs_;
  }

  const ExprPtr& input() const {
    VELOX_CHECK_EQ(1, inputs_.size());
    return inputs_.at(0);
  }

  const ExprPtr& inputAt(size_t index) const {
    VELOX_CHECK_LT(index, inputs_.size());
    return inputs_.at(index);
  }

  const std::optional<std::string>& alias() const {
    return alias_;
  }

  virtual std::string toString() const = 0;

 protected:
  std::string appendAliasIfExists(std::string name) const {
    if (!alias_.has_value()) {
      return name;
    }

    return fmt::format("{} AS {}", std::move(name), alias_.value());
  }

  const std::vector<ExprPtr> inputs_;
  const std::optional<std::string> alias_;
};

} // namespace facebook::velox::core
