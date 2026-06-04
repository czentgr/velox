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
#include "velox/functions/prestosql/types/VarcharNType.h"
#include "velox/functions/prestosql/types/VarcharNRegistration.h"
#include "velox/functions/prestosql/types/tests/TypeTestBase.h"

namespace facebook::velox::test {

class VarcharNTypeTest : public testing::Test, public TypeTestBase {
 public:
  VarcharNTypeTest() {
    registerVarcharNType();
  }
};

TEST_F(VarcharNTypeTest, basic) {
  auto type = VARCHAR_N(10);

  ASSERT_STREQ(type->name(), "VARCHARN");
  ASSERT_STREQ(type->kindName(), "VARCHAR");
  ASSERT_EQ(type->kind(), TypeKind::VARCHAR);
  ASSERT_EQ(type->toString(), "VARCHAR(10)");
  ASSERT_EQ(getVarcharNLength(*type), 10);
  ASSERT_TRUE(isVarcharNType(*type));
  ASSERT_FALSE(isVarcharNType(*VARCHAR()));

  ASSERT_TRUE(hasType("VARCHARN"));
  // Surface name 'varchar(N)' routes to VARCHARN via getType().
  ASSERT_EQ(*getType("VARCHAR", {TypeParameter(int64_t{10})}), *type);
  ASSERT_EQ(*getType("VARCHARN", {TypeParameter(int64_t{10})}), *type);
}

TEST_F(VarcharNTypeTest, equivalent) {
  // Same length is equivalent.
  ASSERT_TRUE(VARCHAR_N(10)->equivalent(*VARCHAR_N(10)));

  // Different length is not equivalent.
  ASSERT_FALSE(VARCHAR_N(10)->equivalent(*VARCHAR_N(20)));

  // VARCHAR(N) is not equivalent to unbounded VARCHAR.
  ASSERT_FALSE(VARCHAR_N(10)->equivalent(*VARCHAR()));
  ASSERT_FALSE(VARCHAR()->equivalent(*VARCHAR_N(10)));
}

TEST_F(VarcharNTypeTest, lengthBoundary) {
  // Zero length is rejected.
  VELOX_ASSERT_THROW(
      VARCHAR_N(0), "VARCHAR(N) length must be greater than zero");

  // Maximum int32 length is allowed.
  auto large = VARCHAR_N(std::numeric_limits<int32_t>::max());
  ASSERT_EQ(getVarcharNLength(*large), std::numeric_limits<int32_t>::max());
}

TEST_F(VarcharNTypeTest, serde) {
  testTypeSerde(VARCHAR_N(1));
  testTypeSerde(VARCHAR_N(10));
  testTypeSerde(VARCHAR_N(std::numeric_limits<int32_t>::max()));
}

} // namespace facebook::velox::test
