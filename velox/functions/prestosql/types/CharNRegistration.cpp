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

#include "velox/functions/prestosql/types/CharNRegistration.h"

#include "velox/expression/CastExpr.h"
#include "velox/functions/prestosql/types/CharNType.h"
#include "velox/functions/prestosql/types/VarcharNType.h"
#include "velox/type/CastRegistry.h"

namespace facebook::velox {
namespace {

// Strips trailing spaces (0x20) from a StringView in-place by returning a
// shorter view over the same buffer.
StringView rtrimSpaces(StringView input) {
  const char* data = input.data();
  size_t size = input.size();
  while (size > 0 && data[size - 1] == ' ') {
    --size;
  }
  return StringView(data, size);
}

// Truncates a StringView to the requested maximum length.
StringView truncate(StringView input, uint32_t maxLength) {
  if (input.size() <= maxLength) {
    return input;
  }
  return StringView(input.data(), maxLength);
}

// Cast operator implementing CHAR(N) semantics. Storage is unpadded — trailing
// spaces are stripped at cast time so byte-exact equality on the stored form
// matches Presto's trailing-space-insensitive CHAR comparison.
//   - VARCHAR     -> CHAR(N): strip trailing spaces, truncate to N bytes.
//   - VARCHAR(M)  -> CHAR(N): strip trailing spaces, truncate to N bytes.
//   - CHAR(M)     -> CHAR(N): truncate to N bytes (already trim-stored).
//   - CHAR(N)     -> VARCHAR: zero-copy passthrough.
//   - CHAR(N)     -> VARCHAR(M): truncate to M bytes.
class CharNCastOperator : public exec::CastOperator {
 public:
  void castTo(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const TypePtr& resultType,
      VectorPtr& result) const override {
    context.ensureWritable(rows, resultType, result);
    VELOX_CHECK_EQ(input.typeKind(), TypeKind::VARCHAR);

    const auto length = getCharNLength(*resultType);
    auto* flatResult = result->as<FlatVector<StringView>>();
    const auto* source = input.as<SimpleVector<StringView>>();

    const bool sourceIsCharN = isCharNType(*input.type());

    context.applyToSelectedNoThrow(rows, [&](auto row) {
      const auto value = source->valueAt(row);
      const auto trimmed = sourceIsCharN ? value : rtrimSpaces(value);
      exec::StringWriter writer(flatResult, row);
      writer.copy_from(truncate(trimmed, length));
      writer.finalize();
    });
  }

  void castFrom(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const TypePtr& resultType,
      VectorPtr& result) const override {
    context.ensureWritable(rows, resultType, result);
    VELOX_CHECK_EQ(resultType->kind(), TypeKind::VARCHAR);

    auto* flatResult = result->as<FlatVector<StringView>>();
    const auto* source = input.as<SimpleVector<StringView>>();

    // CHAR(N) -> VARCHAR(M): truncate to M.
    // CHAR(N) -> VARCHAR  : passthrough.
    const bool resultIsBounded = isVarcharNType(*resultType);
    const uint32_t length =
        resultIsBounded ? getVarcharNLength(*resultType) : 0;

    context.applyToSelectedNoThrow(rows, [&](auto row) {
      const auto value = source->valueAt(row);
      exec::StringWriter writer(flatResult, row);
      writer.copy_from(resultIsBounded ? truncate(value, length) : value);
      writer.finalize();
    });
  }
};

class CharNTypeFactory : public CustomTypeFactory {
 public:
  TypePtr getType(const std::vector<TypeParameter>& parameters) const override {
    VELOX_USER_CHECK_EQ(parameters.size(), 1);
    VELOX_USER_CHECK(parameters[0].kind == TypeParameterKind::kLongLiteral);
    VELOX_USER_CHECK(parameters[0].longLiteral.has_value());
    return CHAR_N(static_cast<uint32_t>(parameters[0].longLiteral.value()));
  }

  exec::CastOperatorPtr getCastOperator() const override {
    return std::make_shared<CharNCastOperator>();
  }

  AbstractInputGeneratorPtr getInputGenerator(
      const InputGeneratorConfig& /*config*/) const override {
    return nullptr;
  }
};

} // namespace

void registerCharNType() {
  registerCustomType("CHARN", std::make_unique<const CharNTypeFactory>());

  registerCastRules({
      // Bounded CHAR -> unbounded VARCHAR is a free type-only widening.
      {.fromType = "CHARN",
       .toType = "VARCHAR",
       .implicitAllowed = true,
       .cost = 1},
      // VARCHAR -> CHAR(N) requires explicit cast (trim + truncate).
      {.fromType = "VARCHAR",
       .toType = "CHARN",
       .implicitAllowed = false,
       .cost = 100},
      // VARCHAR(M) -> CHAR(N) requires explicit cast.
      {.fromType = "VARCHARN",
       .toType = "CHARN",
       .implicitAllowed = false,
       .cost = 100},
      // CHAR(N) -> VARCHAR(M) requires explicit cast.
      {.fromType = "CHARN",
       .toType = "VARCHARN",
       .implicitAllowed = false,
       .cost = 100},
      // CHAR(M) -> CHAR(N) requires explicit cast.
      {.fromType = "CHARN",
       .toType = "CHARN",
       .implicitAllowed = false,
       .cost = 100},
  });
}

} // namespace facebook::velox
