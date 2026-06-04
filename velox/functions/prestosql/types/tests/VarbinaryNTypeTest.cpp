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
#include "velox/functions/prestosql/types/VarbinaryNType.h"
#include "velox/functions/prestosql/types/VarbinaryNRegistration.h"
#include "velox/functions/prestosql/types/tests/TypeTestBase.h"

namespace facebook::velox::test {

class VarbinaryNTypeTest : public testing::Test, public TypeTestBase {
 public:
  VarbinaryNTypeTest() {
    registerVarbinaryNType();
  }
};

TEST_F(VarbinaryNTypeTest, basic) {
  auto type = VARBINARY_N(10);

  ASSERT_STREQ(type->name(), "VARBINARYN");
  ASSERT_STREQ(type->kindName(), "VARBINARY");
  ASSERT_EQ(type->kind(), TypeKind::VARBINARY);
  ASSERT_EQ(type->toString(), "VARBINARY(10)");
  ASSERT_EQ(getVarbinaryNLength(*type), 10);
  ASSERT_TRUE(isVarbinaryNType(*type));
  ASSERT_FALSE(isVarbinaryNType(*VARBINARY()));

  ASSERT_TRUE(hasType("VARBINARYN"));
  // Surface name 'varbinary(N)' routes to VARBINARYN via getType().
  ASSERT_EQ(*getType("VARBINARY", {TypeParameter(int64_t{10})}), *type);
  ASSERT_EQ(*getType("VARBINARYN", {TypeParameter(int64_t{10})}), *type);
}

TEST_F(VarbinaryNTypeTest, equivalent) {
  ASSERT_TRUE(VARBINARY_N(10)->equivalent(*VARBINARY_N(10)));
  ASSERT_FALSE(VARBINARY_N(10)->equivalent(*VARBINARY_N(20)));

  // VARBINARY(N) is not equivalent to unbounded VARBINARY.
  ASSERT_FALSE(VARBINARY_N(10)->equivalent(*VARBINARY()));
  ASSERT_FALSE(VARBINARY()->equivalent(*VARBINARY_N(10)));
}

TEST_F(VarbinaryNTypeTest, lengthBoundary) {
  VELOX_ASSERT_THROW(
      VARBINARY_N(0), "VARBINARY(N) length must be greater than zero");

  auto large = VARBINARY_N(std::numeric_limits<int32_t>::max());
  ASSERT_EQ(getVarbinaryNLength(*large), std::numeric_limits<int32_t>::max());
}

TEST_F(VarbinaryNTypeTest, serde) {
  testTypeSerde(VARBINARY_N(1));
  testTypeSerde(VARBINARY_N(10));
  testTypeSerde(VARBINARY_N(std::numeric_limits<int32_t>::max()));
}

} // namespace facebook::velox::test
