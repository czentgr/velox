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

/// Represents a bounded CHAR(N) type. The physical layout is identical to
/// VARCHAR (StringView). Trailing spaces are stripped at cast time, so the
/// stored value is always "unpadded". Trailing-space-insensitive equality is
/// therefore equivalent to byte-exact equality on the unpadded form. The
/// bound N is enforced at cast time.
class CharNType final : public VarcharType {
 public:
  explicit CharNType(uint32_t length)
      : parameters_{TypeParameter(static_cast<int64_t>(length))} {
    VELOX_USER_CHECK_GT(
        length, 0, "CHAR(N) length must be greater than zero.");
  }

  uint32_t length() const {
    return static_cast<uint32_t>(parameters_[0].longLiteral.value());
  }

  bool equivalent(const Type& other) const override {
    if (!Type::hasSameTypeId(other)) {
      return false;
    }
    const auto* otherType = dynamic_cast<const CharNType*>(&other);
    if (otherType == nullptr) {
      return false;
    }
    return otherType->length() == length();
  }

  const char* name() const override {
    return "CHARN";
  }

  std::string toString() const override {
    return fmt::format("CHAR({})", length());
  }

  std::span<const TypeParameter> parameters() const override {
    return parameters_;
  }

  folly::dynamic serialize() const override {
    auto obj = VarcharType::serialize();
    obj["type"] = name();
    obj["length"] = length();
    return obj;
  }

 private:
  const std::array<TypeParameter, 1> parameters_;
};

FOLLY_ALWAYS_INLINE bool isCharNType(const TypePtr& type) {
  return dynamic_cast<const CharNType*>(type.get()) != nullptr;
}

FOLLY_ALWAYS_INLINE bool isCharNType(const Type& type) {
  return dynamic_cast<const CharNType*>(&type) != nullptr;
}

FOLLY_ALWAYS_INLINE std::shared_ptr<const CharNType> CHAR_N(uint32_t length) {
  return std::make_shared<const CharNType>(length);
}

FOLLY_ALWAYS_INLINE uint32_t getCharNLength(const Type& type) {
  const auto* charN = dynamic_cast<const CharNType*>(&type);
  VELOX_CHECK_NOT_NULL(charN, "Type is not CHAR(N).");
  return charN->length();
}

} // namespace facebook::velox
