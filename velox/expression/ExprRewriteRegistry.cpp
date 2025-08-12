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
#include "velox/expression/ExprRewriteRegistry.h"
#include "velox/core/Expressions.h"
#include "velox/expression/ExprRewrite.h"

namespace facebook::velox::exec {

folly::Synchronized<std::vector<expression::ExpressionRewrite>>
    ExpressionRewriteRegistry::kRewrites_;

// static
core::TypedExprPtr ExpressionRewriteRegistry::applyRewrite(
    std::function<core::TypedExprPtr(const expression::ExpressionRewrite&)>
        apply) {
  auto lockedRewrites = kRewrites_.rlock();
  for (const auto& rewrite : *lockedRewrites) {
    if (auto result = apply(rewrite)) {
      VLOG(3) << fmt::format(
          "Applied rewrite '{}' to expression ", rewrite.name());
      return result;
    }
  }
  return nullptr;
}

// static
void ExpressionRewriteRegistry::registerExpressionRewrite(
    expression::ExpressionRewrite rewrite) {
  auto lockedRewrites = kRewrites_.wlock();
  lockedRewrites->emplace_back(rewrite);
}

// static
void ExpressionRewriteRegistry::unregisterExpressionRewrites() {
  auto lockedRewrites = kRewrites_.wlock();
  lockedRewrites->clear();
}

} // namespace facebook::velox::exec
