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

#include "velox/type/SimpleFunctionApi.h"
#include "velox/type/Type.h"

namespace facebook::velox {

/// Represents a bounded VARBINARY(N) type. The physical layout is identical to
/// VARBINARY (StringView). The bound N is enforced at cast time.
class VarbinaryNType final : public VarbinaryType {
 public:
  explicit VarbinaryNType(uint32_t length)
      : parameters_{TypeParameter(static_cast<int64_t>(length))} {
    VELOX_USER_CHECK_GT(
        length, 0, "VARBINARY(N) length must be greater than zero.");
  }

  uint32_t length() const {
    return static_cast<uint32_t>(parameters_[0].longLiteral.value());
  }

  bool equivalent(const Type& other) const override {
    if (!Type::hasSameTypeId(other)) {
      return false;
    }
    const auto* otherType = dynamic_cast<const VarbinaryNType*>(&other);
    if (otherType == nullptr) {
      return false;
    }
    return otherType->length() == length();
  }

  const char* name() const override {
    return "VARBINARYN";
  }

  std::string toString() const override {
    return fmt::format("VARBINARY({})", length());
  }

  std::span<const TypeParameter> parameters() const override {
    return parameters_;
  }

  folly::dynamic serialize() const override {
    auto obj = VarbinaryType::serialize();
    obj["type"] = name();
    obj["length"] = length();
    return obj;
  }

 private:
  const std::array<TypeParameter, 1> parameters_;
};

FOLLY_ALWAYS_INLINE bool isVarbinaryNType(const TypePtr& type) {
  return dynamic_cast<const VarbinaryNType*>(type.get()) != nullptr;
}

FOLLY_ALWAYS_INLINE bool isVarbinaryNType(const Type& type) {
  return dynamic_cast<const VarbinaryNType*>(&type) != nullptr;
}

FOLLY_ALWAYS_INLINE std::shared_ptr<const VarbinaryNType> VARBINARY_N(
    uint32_t length) {
  return std::make_shared<const VarbinaryNType>(length);
}

FOLLY_ALWAYS_INLINE uint32_t getVarbinaryNLength(const Type& type) {
  const auto* varbinaryN = dynamic_cast<const VarbinaryNType*>(&type);
  VELOX_CHECK_NOT_NULL(varbinaryN, "Type is not VARBINARY(N).");
  return varbinaryN->length();
}

} // namespace facebook::velox
