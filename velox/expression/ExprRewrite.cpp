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

#include <set>

#include "velox/expression/ExprConstants.h"
#include "velox/expression/ExprRewrite.h"
#include "velox/expression/ExprRewriteRegistry.h"
#include "velox/expression/ExprUtils.h"

namespace facebook::velox::expression {

namespace {
enum class ConstantEvalResult {
  IS_NOT_CONSTANT = 0,
  IS_NULL,
  IS_TRUE,
  IS_FALSE
};

ConstantEvalResult evalExprAsConstant(const core::TypedExprPtr& expr) {
  if (expr->isConstantKind()) {
    auto constantExpr = expr->asUnchecked<core::ConstantTypedExpr>();
    if (constantExpr->isNull()) {
      return ConstantEvalResult::IS_NULL;
    }
    auto value = constantExpr->hasValueVector()
        ? constantExpr->valueVector()->as<ConstantVector<bool>>()->valueAt(0)
        : constantExpr->value().value<TypeKind::BOOLEAN>();
    if (value) {
      return ConstantEvalResult::IS_TRUE;
    }
    return ConstantEvalResult::IS_FALSE;
  }
  return ConstantEvalResult::IS_NOT_CONSTANT;
}
} // namespace

// Input expression should be of form: IF(condition, then, else).
core::TypedExprPtr rewriteIfExpression(
    const core::TypedExprPtr& input,
    const std::shared_ptr<core::QueryCtx>& queryCtx,
    memory::MemoryPool* pool) {
  /*
  auto expr = std::dynamic_pointer_cast<const core::CallTypedExpr>(input);
  if (expr == nullptr || expr->name() != kIf || expr->inputs().size() != 3) {
    return nullptr;
  }
  */

  if (!utils::isCall(input, kIf) || input->inputs().size() != 3) {
    return nullptr;
  }
  // const core::CallTypedExprPtr expr =
  // input->asUnchecked<core::CallTypedExpr>();
  auto expr = std::dynamic_pointer_cast<const core::CallTypedExpr>(input);

  const auto& condition = expr->inputs().at(0);
  auto foldedCondition = utils::tryConstantFold(condition, queryCtx, pool);
  // The folded expression could be the fail function. In this case,
  // we don't want to futher analyze the expression and instead return the fail
  // function expression.
  if (utils::isCall(foldedCondition, kFail)) {
    return foldedCondition;
  }
  const auto result = evalExprAsConstant(foldedCondition);
  switch (result) {
    case ConstantEvalResult::IS_NULL:
      [[fallthrough]];
    case ConstantEvalResult::IS_TRUE:
      return expr->inputs().at(1);
    case ConstantEvalResult::IS_FALSE:
      return expr->inputs().at(2);
    case ConstantEvalResult::IS_NOT_CONSTANT:
      [[fallthrough]];
    default:
      return expr;
  }
  return expr;
}

// Input expression should be of form: SWITCH(condition1, value1, condition2,
//   value2, ...., defaultValue).
core::TypedExprPtr rewriteSwitchExpression(
    const core::TypedExprPtr& input,
    const std::shared_ptr<core::QueryCtx>& queryCtx,
    memory::MemoryPool* pool) {
  /*
  auto expr = std::dynamic_pointer_cast<const core::CallTypedExpr>(input);
  if (expr == nullptr || expr->name() != kSwitch) {
    return nullptr;
  }
  */
  if (!utils::isCall(input, kSwitch)) {
    return nullptr;
  }
  // const core::CallTypedExprPtr expr =
  // input->asUnchecked<core::CallTypedExpr>();
  auto expr = std::dynamic_pointer_cast<const core::CallTypedExpr>(input);

  const auto& inputs = expr->inputs();
  const auto numInputs = inputs.size();
  std::vector<core::TypedExprPtr> optimizedInputs;
  // If a case evaluates to true, it will be the new else clause.
  bool hasOptimizedElseValue = false;
  for (auto i = 0; i < numInputs - 1; i += 2) {
    const auto& condition = inputs.at(i);
    const auto foldedCondition =
        utils::tryConstantFold(condition, queryCtx, pool);
    if (utils::isCall(foldedCondition, kFail)) {
      return foldedCondition;
    }

    const auto& value = expr->inputs().at(i + 1);
    const auto foldedValue = utils::tryConstantFold(value, queryCtx, pool);
    if (utils::isCall(foldedValue, kFail)) {
      return foldedValue;
    }

    const auto result = evalExprAsConstant(foldedCondition);
    switch (result) {
      case ConstantEvalResult::IS_NULL:
        continue;
      case ConstantEvalResult::IS_TRUE:
        if (optimizedInputs.empty()) {
          return foldedValue;
        }
        hasOptimizedElseValue = true;
        optimizedInputs.emplace_back(foldedValue);
        break;
      case ConstantEvalResult::IS_FALSE:
        return expr->inputs().at(2);
      case ConstantEvalResult::IS_NOT_CONSTANT:
        optimizedInputs.emplace_back(foldedCondition);
        optimizedInputs.emplace_back(foldedValue);
        continue;
      default:
        return expr;
    }
    break;

    /*
        if (auto constantExpr =
                std::dynamic_pointer_cast<const core::ConstantTypedExpr>(
                    foldedCondition)) {
          if (auto constVector = constantExpr->toConstantVector(pool)) {
            if (!constVector->isNullAt(0) &&
                constVector->as<ConstantVector<bool>>()->valueAt(0)) {
              if (optimizedInputs.empty()) {
                return foldedValue;
              }
              hasOptimizedElseValue = true;
              optimizedInputs.emplace_back(foldedValue);
              break;
            }
          }
        } else {
          optimizedInputs.emplace_back(foldedCondition);
          optimizedInputs.emplace_back(foldedValue);
        }
    */
  }

  if (!hasOptimizedElseValue) {
    const auto foldedElseValue =
        utils::tryConstantFold(inputs.at(numInputs - 1), queryCtx, pool);
    if (utils::isCall(foldedElseValue, kFail)) {
      return foldedElseValue;
    }
    if (optimizedInputs.empty()) {
      return foldedElseValue;
    }
    optimizedInputs.emplace_back(foldedElseValue);
  }
  return std::make_shared<core::CallTypedExpr>(
      expr->type(), std::move(optimizedInputs), expr->name());
}

// When all input literals in IN-list are constant, the expression is expected
// to be of type IN(value, arrayVector<literal1, ....., literalN>). When any
// input literal in IN-list is non-constant, the expression is expected to be
// of type IN(value, literal1, ....., literalN). The latter case is optimized
// by this function and the former is handled during constant folding.
core::TypedExprPtr rewriteInExpression(
    const core::TypedExprPtr& input,
    const std::shared_ptr<core::QueryCtx>& queryCtx,
    memory::MemoryPool* pool) {
  /*
    auto expr = std::dynamic_pointer_cast<const core::CallTypedExpr>(input);
    if (expr == nullptr || expr->name() != kIn || expr->inputs().size() < 2) {
      return nullptr;
    }
  */
  if (!utils::isCall(input, kIn) || input->inputs().size() < 2) {
    return nullptr;
  }
  // const core::CallTypedExprPtr expr =
  // input->asUnchecked<core::CallTypedExpr>();
  auto expr = std::dynamic_pointer_cast<const core::CallTypedExpr>(input);

  const auto& value = expr->inputs().at(0);
  const auto foldedExpr = utils::tryConstantFold(value, queryCtx, pool);
  if (utils::isCall(foldedExpr, kFail)) {
    return foldedExpr;
  }

  if (auto constantExpr =
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(
              foldedExpr)) {
    const auto& inList = expr->inputs().at(1);
    if (std::dynamic_pointer_cast<const core::ConstantTypedExpr>(inList) ==
        nullptr) {
      const auto& inputs = expr->inputs();
      const auto numInputs = inputs.size();
      std::vector<core::TypedExprPtr> optimizedInputs;
      optimizedInputs.emplace_back(foldedExpr);

      for (auto i = 1; i < numInputs; i++) {
        const auto& literal = inputs.at(i);
        const auto foldedLiteral =
            utils::tryConstantFold(literal, queryCtx, pool);
        if (utils::isCall(foldedLiteral, kFail)) {
          return foldedLiteral;
        }
        if (auto constantLiteral =
                std::dynamic_pointer_cast<const core::ConstantTypedExpr>(
                    foldedLiteral)) {
          if (constantExpr->toConstantVector(pool)->equalValueAt(
                  constantLiteral->toConstantVector(pool).get(), 0, 0)) {
            return std::make_shared<core::ConstantTypedExpr>(BOOLEAN(), true);
          }
        } else {
          optimizedInputs.emplace_back(foldedLiteral);
        }
      }
      return std::make_shared<core::CallTypedExpr>(
          expr->type(), std::move(optimizedInputs), expr->name());
    }
  }
  return expr;
}

core::TypedExprPtr rewriteConjunctExpression(
    const core::TypedExprPtr& input,
    const std::shared_ptr<core::QueryCtx>& queryCtx,
    memory::MemoryPool* pool) {
  /*
  auto expr = std::dynamic_pointer_cast<const core::CallTypedExpr>(input);
  if (expr == nullptr || (expr->name() != kAnd && expr->name() != kOr)) {
    return nullptr;
  }
  */
  if (!utils::isCall(input, kAnd) && !utils::isCall(input, kOr)) {
    return nullptr;
  }
  // const core::CallTypedExprPtr expr = std::dynamic_pointer_cast<const
  // core::CallTypedExpr>(input);
  auto expr = std::dynamic_pointer_cast<const core::CallTypedExpr>(input);

  const bool isAnd = (expr->name() == kAnd) ? true : false;

  // If all inputs are AND or OR then we can flatten the inputs into a vector
  // before further optimizing.
  auto canFlatten = utils::allInputTypesEquivalent(expr);
  std::vector<core::TypedExprPtr> flat;
  if (canFlatten) {
    utils::flattenInput(input, expr->name(), flat);
  }

  const auto& inputsToOptimize = canFlatten ? flat : expr->inputs();
  bool allInputsConstant = true;
  bool hasNullInput = false;
  std::vector<core::TypedExprPtr> optimizedInputs;
  core::TypedExprPtr nullInput = nullptr;
  for (const auto& inputExpr : inputsToOptimize) {
    auto foldedExpr = utils::tryConstantFold(inputExpr, queryCtx, pool);
    if (utils::isCall(foldedExpr, kFail)) {
      return foldedExpr;
    }

    const auto result = evalExprAsConstant(foldedExpr);
    switch (result) {
      case ConstantEvalResult::IS_NULL:
        if (!hasNullInput) {
          hasNullInput = true;
          nullInput = inputExpr;
        }
        break;
      case ConstantEvalResult::IS_TRUE:
        if (!isAnd) {
          // OR (.., true, ..) -> true
          return foldedExpr;
        }
        break;
      case ConstantEvalResult::IS_FALSE:
        if (isAnd) {
          // AND (.., false, ..) -> false
          return foldedExpr;
        }
        break;
      case ConstantEvalResult::IS_NOT_CONSTANT:
        allInputsConstant = false;
        optimizedInputs.push_back(inputExpr);
        break;
      default:
        return expr;
    }

    /*
    if (auto constantExpr =
            std::dynamic_pointer_cast<const core::ConstantTypedExpr>(
                foldedExpr)) {
      auto constantVector = constantExpr->toConstantVector(pool);
      if (!constantVector->isNullAt(0)) {
        if (isAnd) {
          // AND (.., false, ..) -> false
          if (!constantVector->as<ConstantVector<bool>>()->valueAt(0)) {
            return constantExpr;
          }
        } else {
          // OR (.., true, ..) -> true
          if (constantVector->as<ConstantVector<bool>>()->valueAt(0)) {
            return constantExpr;
          }
        }
      } else if (!hasNullInput) {
        hasNullInput = true;
        nullInput = inputExpr;
      } // else do nothing because we encountered a NULL that we already dealt
        // with.
    } else {
      allInputsConstant = false;
      optimizedInputs.push_back(inputExpr);
    }
    */
  }

  if (allInputsConstant && hasNullInput) {
    return nullInput;
  } else if (optimizedInputs.empty()) {
    return expr->inputs().front();
  } else if (optimizedInputs.size() == 1) {
    return optimizedInputs.front();
  }
  return std::make_shared<core::CallTypedExpr>(
      expr->type(), std::move(optimizedInputs), expr->name());
}

/// Comparator for core::TypedExprPtr; used to deduplicate arguments to
/// COALESCE special form expression.
struct TypedExprComparator {
  bool operator()(const core::TypedExprPtr& a, const core::TypedExprPtr& b)
      const {
    return a->hash() < b->hash();
  }
};

core::TypedExprPtr addCoalesceArgument(
    const core::TypedExprPtr& input,
    std::set<core::TypedExprPtr, TypedExprComparator>& optimizedTypedExprs,
    std::vector<core::TypedExprPtr>& deduplicatedInputs,
    const std::shared_ptr<core::QueryCtx>& queryCtx,
    memory::MemoryPool* pool) {
  auto foldedExpr = utils::tryConstantFold(input, queryCtx, pool);
  if (utils::isCall(foldedExpr, kFail)) {
    return foldedExpr;
  }

  // First non-NULL constant input to COALESCE returns non-NULL value.
  if (foldedExpr->isConstantKind()) {
    auto constantExpr =
        std::dynamic_pointer_cast<const core::ConstantTypedExpr>(foldedExpr);
    if (!constantExpr->isNull()) {
      if (optimizedTypedExprs.find(foldedExpr) == optimizedTypedExprs.end()) {
        optimizedTypedExprs.insert(foldedExpr);
        deduplicatedInputs.push_back(input);
      }
      return input;
    }
  } else if (
      optimizedTypedExprs.find(foldedExpr) == optimizedTypedExprs.end()) {
    optimizedTypedExprs.insert(foldedExpr);
    deduplicatedInputs.push_back(input);
  }

  /*
    if (auto constantExpr =
            std::dynamic_pointer_cast<const core::ConstantTypedExpr>(
                foldedExpr)) {
      auto constantVector = constantExpr->toConstantVector(pool);
      if (!constantVector->isNullAt(0)) {
        if (optimizedTypedExprs.find(foldedExpr) == optimizedTypedExprs.end()) {
          optimizedTypedExprs.insert(foldedExpr);
          deduplicatedInputs.push_back(input);
        }
        return input;
      }
    } else if (
      optimizedTypedExprs.find(foldedExpr) == optimizedTypedExprs.end()) {
      optimizedTypedExprs.insert(foldedExpr);
      deduplicatedInputs.push_back(input);
    }
  */

  return nullptr;
}

core::TypedExprPtr rewriteCoalesceSpecialFormImpl(
    const core::CallTypedExprPtr& expr,
    std::set<core::TypedExprPtr, TypedExprComparator>& inputTypedExprSet,
    std::vector<core::TypedExprPtr>& deduplicatedInputs,
    const std::shared_ptr<core::QueryCtx>& queryCtx,
    memory::MemoryPool* pool) {
  // Once a constant input is seen, subsequent inputs to the COALESCE expression
  // can be ignored.
  for (const auto& input : expr->inputs()) {
    if (const auto call =
            std::dynamic_pointer_cast<const core::CallTypedExpr>(input)) {
      if (call->name() == kCoalesce) {
        // If the argument is a COALESCE expression, the arguments of inner
        // COALESCE can be combined with the arguments of outer COALESCE
        // expression. If the inner COALESCE has a constant expression, return.
        if (auto optimizedCoalesceSubExpr = rewriteCoalesceSpecialFormImpl(
                call, inputTypedExprSet, deduplicatedInputs, queryCtx, pool)) {
          return optimizedCoalesceSubExpr;
        }
      } else if (
          auto optimized = addCoalesceArgument(
              input, inputTypedExprSet, deduplicatedInputs, queryCtx, pool)) {
        return optimized;
      }
    } else if (
        auto optimized = addCoalesceArgument(
            input, inputTypedExprSet, deduplicatedInputs, queryCtx, pool)) {
      return optimized;
    }
  }
  // Return null if COALESCE has no constant input.
  return nullptr;
}

core::TypedExprPtr rewriteCoalesceExpression(
    const core::TypedExprPtr& input,
    const std::shared_ptr<core::QueryCtx>& queryCtx,
    memory::MemoryPool* pool) {
  /*
  auto call = std::dynamic_pointer_cast<const core::CallTypedExpr>(expr);
  if (call == nullptr || call->name() != kCoalesce) {
    return nullptr;
  }
  */
  if (!utils::isCall(input, kCoalesce)) {
    return nullptr;
  }
  // const core::CallTypedExprPtr call =
  // input->asUnchecked<core::CallTypedExpr>();
  auto call = std::dynamic_pointer_cast<const core::CallTypedExpr>(input);

  // Deduplicate inputs to COALESCE and remove NULL inputs, returning a list of
  // optimized inputs to COALESCE.
  std::set<core::TypedExprPtr, TypedExprComparator> inputTypedExprSet;
  std::vector<core::TypedExprPtr> deduplicatedInputs;
  rewriteCoalesceSpecialFormImpl(
      call, inputTypedExprSet, deduplicatedInputs, queryCtx, pool);

  // Return NULL if all inputs to COALESCE are NULL. If there is a single input
  // to COALESCE after optimization, return this expression. Otherwise, return
  // COALESCE expression with optimized inputs.
  if (deduplicatedInputs.empty()) {
    return call->inputs().front();
  } else if (deduplicatedInputs.size() == 1) {
    return deduplicatedInputs.front();
  }
  return std::make_shared<core::CallTypedExpr>(
      call->type(), std::move(deduplicatedInputs), call->name());
}

core::TypedExprPtr rewriteExpression(
    const core::TypedExprPtr& expr,
    const std::shared_ptr<core::QueryCtx>& queryCtx,
    memory::MemoryPool* pool) {
  auto result = exec::ExpressionRewriteRegistry::applyRewrite(
      [&](const ExpressionRewrite& rewrite) -> core::TypedExprPtr {
        if (auto rewritten = rewrite(expr, queryCtx, pool)) {
          return rewritten;
        }
        return nullptr;
      });
  return result;
}

} // namespace facebook::velox::expression
