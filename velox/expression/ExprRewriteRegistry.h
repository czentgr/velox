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

#include <vector>

#include "folly/Synchronized.h"
#include "velox/expression/ExprRewrite.h"

namespace facebook::velox::exec {

class ExpressionRewriteRegistry {
 public:
  static core::TypedExprPtr applyRewrite(
      std::function<core::TypedExprPtr(const expression::ExpressionRewrite&)>
          apply);

  /// Appends a 'rewrite' to 'expressionRewrites'.
  ///
  /// The logic that applies re-writes is very simple and assumes that all
  /// rewrites are independent. Re-writes are applied to all expressions
  /// starting at the root and going down the hierarchy. For each expression,
  /// rewrites are applied in the order they were registered. The first rewrite
  /// that returns non-null result terminates the re-write for this particular
  /// expression.
  static void registerExpressionRewrite(expression::ExpressionRewrite rewrite);

  /// Clears all registered expression re-writes.
  static void unregisterExpressionRewrites();

 private:
  static folly::Synchronized<std::vector<expression::ExpressionRewrite>>
      kRewrites_;
};
} // namespace facebook::velox::exec
