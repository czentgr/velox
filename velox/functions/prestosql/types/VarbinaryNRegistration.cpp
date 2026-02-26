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

#include "velox/functions/prestosql/types/VarbinaryNRegistration.h"

#include "velox/expression/CastExpr.h"
#include "velox/functions/prestosql/types/VarbinaryNType.h"
#include "velox/type/CastRegistry.h"

namespace facebook::velox {
namespace {

// Truncates a StringView to the requested maximum length.
StringView truncate(StringView input, uint32_t maxLength) {
  if (input.size() <= maxLength) {
    return input;
  }
  return StringView(input.data(), maxLength);
}

class VarbinaryNCastOperator : public exec::CastOperator {
 public:
  void castTo(
      const BaseVector& input,
      exec::EvalCtx& context,
      const SelectivityVector& rows,
      const TypePtr& resultType,
      VectorPtr& result) const override {
    context.ensureWritable(rows, resultType, result);
    VELOX_CHECK_EQ(input.typeKind(), TypeKind::VARBINARY);

    const auto length = getVarbinaryNLength(*resultType);
    auto* flatResult = result->as<FlatVector<StringView>>();
    const auto* source = input.as<SimpleVector<StringView>>();

    context.applyToSelectedNoThrow(rows, [&](auto row) {
      const auto value = source->valueAt(row);
      exec::StringWriter writer(flatResult, row);
      writer.copy_from(truncate(value, length));
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
    VELOX_CHECK_EQ(resultType->kind(), TypeKind::VARBINARY);

    auto* flatResult = result->as<FlatVector<StringView>>();
    const auto* source = input.as<SimpleVector<StringView>>();

    context.applyToSelectedNoThrow(rows, [&](auto row) {
      const auto value = source->valueAt(row);
      exec::StringWriter writer(flatResult, row);
      writer.copy_from(value);
      writer.finalize();
    });
  }
};

class VarbinaryNTypeFactory : public CustomTypeFactory {
 public:
  TypePtr getType(const std::vector<TypeParameter>& parameters) const override {
    VELOX_USER_CHECK_EQ(parameters.size(), 1);
    VELOX_USER_CHECK(parameters[0].kind == TypeParameterKind::kLongLiteral);
    VELOX_USER_CHECK(parameters[0].longLiteral.has_value());
    return VARBINARY_N(
        static_cast<uint32_t>(parameters[0].longLiteral.value()));
  }

  exec::CastOperatorPtr getCastOperator() const override {
    return std::make_shared<VarbinaryNCastOperator>();
  }

  AbstractInputGeneratorPtr getInputGenerator(
      const InputGeneratorConfig& /*config*/) const override {
    return nullptr;
  }
};

} // namespace

void registerVarbinaryNType() {
  registerCustomType(
      "VARBINARYN", std::make_unique<const VarbinaryNTypeFactory>());

  registerCastRules({
      {.fromType = "VARBINARYN",
       .toType = "VARBINARY",
       .implicitAllowed = true,
       .cost = 1},
      {.fromType = "VARBINARY",
       .toType = "VARBINARYN",
       .implicitAllowed = false,
       .cost = 100},
      {.fromType = "VARBINARYN",
       .toType = "VARBINARYN",
       .implicitAllowed = false,
       .cost = 100},
  });
}

} // namespace facebook::velox
