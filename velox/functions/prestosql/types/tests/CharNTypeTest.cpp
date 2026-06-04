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
#include "velox/functions/prestosql/types/CharNType.h"
#include "velox/functions/prestosql/types/CharNRegistration.h"
#include "velox/functions/prestosql/types/VarcharNType.h"
#include "velox/functions/prestosql/types/tests/TypeTestBase.h"

namespace facebook::velox::test {

class CharNTypeTest : public testing::Test, public TypeTestBase {
 public:
  CharNTypeTest() {
    registerCharNType();
  }
};

TEST_F(CharNTypeTest, basic) {
  auto type = CHAR_N(5);

  ASSERT_STREQ(type->name(), "CHARN");
  ASSERT_STREQ(type->kindName(), "VARCHAR");
  // CHAR(N) extends VarcharType for shared physical layout.
  ASSERT_EQ(type->kind(), TypeKind::VARCHAR);
  ASSERT_EQ(type->toString(), "CHAR(5)");
  ASSERT_EQ(getCharNLength(*type), 5);
  ASSERT_TRUE(isCharNType(*type));
  ASSERT_FALSE(isCharNType(*VARCHAR()));

  ASSERT_TRUE(hasType("CHARN"));
  // Surface name 'char(N)' routes to CHARN via getType().
  ASSERT_EQ(*getType("CHAR", {TypeParameter(int64_t{5})}), *type);
  ASSERT_EQ(*getType("CHARN", {TypeParameter(int64_t{5})}), *type);
}

TEST_F(CharNTypeTest, equivalent) {
  ASSERT_TRUE(CHAR_N(10)->equivalent(*CHAR_N(10)));
  ASSERT_FALSE(CHAR_N(10)->equivalent(*CHAR_N(20)));

  // CHAR(N) and VARCHAR(N) of equal length share kind() == VARCHAR but are
  // distinct types.
  ASSERT_FALSE(CHAR_N(10)->equivalent(*VARCHAR_N(10)));
  ASSERT_FALSE(VARCHAR_N(10)->equivalent(*CHAR_N(10)));

  // CHAR(N) is not equivalent to unbounded VARCHAR.
  ASSERT_FALSE(CHAR_N(10)->equivalent(*VARCHAR()));
}

TEST_F(CharNTypeTest, lengthBoundary) {
  VELOX_ASSERT_THROW(CHAR_N(0), "CHAR(N) length must be greater than zero");

  auto large = CHAR_N(std::numeric_limits<int32_t>::max());
  ASSERT_EQ(getCharNLength(*large), std::numeric_limits<int32_t>::max());
}

TEST_F(CharNTypeTest, serde) {
  testTypeSerde(CHAR_N(1));
  testTypeSerde(CHAR_N(10));
  testTypeSerde(CHAR_N(std::numeric_limits<int32_t>::max()));
}

} // namespace facebook::velox::test
