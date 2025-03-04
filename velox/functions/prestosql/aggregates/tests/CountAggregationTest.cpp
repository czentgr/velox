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
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"

using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::aggregate::test {

namespace {

class CountAggregationTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
  }

  RowTypePtr rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"},
          {BIGINT(),
           SMALLINT(),
           INTEGER(),
           BIGINT(),
           REAL(),
           DOUBLE(),
           VARCHAR(),
           TINYINT()})};
};

TEST_F(CountAggregationTest, count) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);

  testAggregations(vectors, {}, {"count()"}, "SELECT count(1) FROM tmp");

  testAggregations(vectors, {}, {"count(1)"}, "SELECT count(1) FROM tmp");

  // count over column with nulls; only non-null rows should be counted
  testAggregations(vectors, {}, {"count(c1)"}, "SELECT count(c1) FROM tmp");

  // count over zero rows; the result should be 0, not null
  testAggregations(
      [&](PlanBuilder& builder) {
        builder.values(vectors).filter("c0 % 3 > 5");
      },
      {},
      {"count(c1)"},
      "SELECT count(c1) FROM tmp WHERE c0 % 3 > 5");

  testAggregations(
      [&](PlanBuilder& builder) {
        builder.values(vectors).project({"c0 % 10 AS c0_mod_10", "c1"});
      },
      {"c0_mod_10"},
      {"count(1)"},
      "SELECT c0 % 10, count(1) FROM tmp GROUP BY 1");

  testAggregations(
      [&](PlanBuilder& builder) {
        builder.values(vectors).project({"c0 % 10 AS c0_mod_10", "c7"});
      },
      {"c0_mod_10"},
      {"count(c7)"},
      "SELECT c0 % 10, count(c7) FROM tmp GROUP BY 1");
}

TEST_F(CountAggregationTest, mask) {
  std::vector<RowVectorPtr> data;
  // Make batches where some batches have mask all true, some half and half and
  // some all false. All batches repeat the same grouping keys. The data to
  // count is null for 1/3 of the rows.
  constexpr int32_t kNumBatches = 10;
  constexpr int32_t kRowsInBatch = 100;
  for (auto counter = 0; counter < kNumBatches; ++counter) {
    data.push_back(makeRowVector(
        {"k", "c", "m"},
        {makeFlatVector<int64_t>(
             kRowsInBatch, [](auto row) { return row / 10; }),
         makeFlatVector<int64_t>(
             kRowsInBatch,
             [](auto row) { return row; },
             [](auto row) { return row % 3 == 0; }),
         makeFlatVector<bool>(kRowsInBatch, [&](auto row) {
           return counter % 3 == 0 ? false
               : counter % 3 == 1  ? false
                                   : row % 2 == 0;
         })}));
  }

  createDuckDbTable(data);

  // count(c)
  auto plan = PlanBuilder()
                  .values(data)
                  .singleAggregation({}, {"count(c)"}, {"m"})
                  .planNode();
  assertQuery(plan, "SELECT count(c) FILTER (where m) FROM tmp");

  plan = PlanBuilder()
             .values(data)
             .partialAggregation({}, {"count(c)"}, {"m"})
             .finalAggregation()
             .planNode();
  assertQuery(plan, "SELECT count(c) FILTER (where m) FROM tmp");

  // count(1)
  plan = PlanBuilder()
             .values(data)
             .singleAggregation({}, {"count()"}, {"m"})
             .planNode();
  assertQuery(plan, "SELECT count(1) FILTER (where m) FROM tmp");

  plan = PlanBuilder()
             .values(data)
             .partialAggregation({}, {"count()"}, {"m"})
             .finalAggregation()
             .planNode();
  assertQuery(plan, "SELECT count(1) FILTER (where m) FROM tmp");

  core::PlanNodeId partialNodeId;
  plan = PlanBuilder()
             .values(data)
             .partialAggregation({"k"}, {"count(c)"}, {"m"})
             .capturePlanNodeId(partialNodeId)
             .finalAggregation()
             .planNode();
  auto task =
      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .maxDrivers(1)
          .config(core::QueryConfig::kAbandonPartialAggregationMinRows, "1")
          .config(core::QueryConfig::kAbandonPartialAggregationMinPct, "0")
          .assertResults(
              "SELECT k, count(c) FILTER (where m) FROM tmp GROUP BY k");
  auto taskStats = toPlanStats(task->taskStats());
  auto partialStats = taskStats.at(partialNodeId).customStats;
  EXPECT_LT(0, partialStats.at("abandonedPartialAggregation").count);
}

TEST_F(CountAggregationTest, distinct) {
  static const auto kNaN = std::numeric_limits<double>::quiet_NaN();
  static const auto kSNaN = std::numeric_limits<double>::signaling_NaN();
  auto data = makeRowVector({
      makeFlatVector<int16_t>({1, 2, 1, 2, 1, 1, 2, 2}),
      makeFlatVector<int32_t>({1, 1, 1, 2, 1, 1, 1, 2}),
      makeNullableFlatVector<int64_t>(
          {std::nullopt, 1, std::nullopt, 2, std::nullopt, 1, std::nullopt, 1}),
      makeNullConstant(TypeKind::DOUBLE, 8),
      makeFlatVector<int128_t>({1, 2, 1, 2, 1, 2, 1, 1}, DECIMAL(38, 8)),
      // Test for NaN equivalence
      makeFlatVector<double>({kNaN, kNaN, kSNaN, kSNaN, 1, 1, 1, 2}),
  });
  createDuckDbTable({data});

  // Global aggregation.
  auto testGlobal = [&](const std::string& input) {
    auto plan =
        PlanBuilder()
            .values({data})
            .singleAggregation({}, {fmt::format("count(distinct {})", input)})
            .planNode();
    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .assertResults(
            fmt::format("SELECT count(distinct {}) FROM tmp", input));
  };

  testGlobal("c1");
  testGlobal("c2");
  testGlobal("c3");
  testGlobal("c4");

  auto plan = PlanBuilder()
                  .values({data})
                  .singleAggregation(
                      {},
                      {"count(distinct c1)",
                       "count(c1)",
                       "count(distinct c2)",
                       "count(c3)"})
                  .planNode();
  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .assertResults(
          "SELECT count(distinct c1), count(c1), count(distinct c2), count(c3) FROM tmp");

  // Global. Empty input.
  plan = PlanBuilder()
             .values({makeRowVector(ROW({"c0"}, {BIGINT()}), 0)})
             .singleAggregation({}, {"count(distinct c0)"})
             .planNode();
  AssertQueryBuilder(plan, duckDbQueryRunner_).assertResults("SELECT 0");

  // Group by.
  auto testGroupBy = [&](const std::string& input) {
    auto plan = PlanBuilder()
                    .values({data})
                    .singleAggregation(
                        {"c0"}, {fmt::format("count(distinct {})", input)})
                    .planNode();
    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .assertResults(fmt::format(
            "SELECT c0, count(distinct {}) FROM tmp GROUP BY 1", input));
  };

  testGroupBy("c1");
  testGroupBy("c2");
  testGroupBy("c3");
  testGroupBy("c4");

  plan = PlanBuilder()
             .values({data})
             .singleAggregation(
                 {"c0"},
                 {"count(distinct c1)",
                  "count(c1)",
                  "count(distinct c2)",
                  "count(c3)"})
             .planNode();
  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .assertResults(
          "SELECT c0, count(distinct c1), count(c1), count(distinct c2), count(c3) FROM tmp GROUP BY 1");

  // Group by. Empty input.
  plan =
      PlanBuilder()
          .values({makeRowVector(ROW({"c0", "c1"}, {BIGINT(), VARCHAR()}), 0)})
          .singleAggregation({"c0"}, {"count(distinct c1)"})
          .planNode();
  AssertQueryBuilder(plan, duckDbQueryRunner_).assertEmptyResults();
}

TEST_F(CountAggregationTest, distinctMask) {
  auto data = makeRowVector({
      makeFlatVector<int16_t>({1, 2, 1, 2, 1, 1, 2, 2}),
      makeFlatVector<bool>(
          {true, false, false, true, false, true, false, true}),
      makeFlatVector<int32_t>({1, -1, -1, 2, -1, 1, -1, 1}),
  });
  createDuckDbTable({data});

  // Global.
  auto plan = PlanBuilder()
                  .values({data})
                  .singleAggregation({}, {"count(distinct c2)"}, {"c1"})
                  .planNode();
  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .assertResults("SELECT count(distinct c2) FILTER (WHERE c1) FROM tmp");

  // Group by.
  plan = PlanBuilder()
             .values({data})
             .singleAggregation({"c0"}, {"count(distinct c2)"}, {"c1"})
             .planNode();
  AssertQueryBuilder(plan, duckDbQueryRunner_)
      .assertResults(
          "SELECT c0, count(distinct c2) FILTER (WHERE c1) FROM tmp GROUP BY 1");
}

void testGlobalAggregation(
    const std::string& col,
    const RowVectorPtr& input,
    const RowVectorPtr& expected) {
  auto globalAggPlan =
      PlanBuilder()
          .values({input})
          .singleAggregation({}, {fmt::format("count(distinct {})", col)})
          .planNode();
  AssertQueryBuilder(globalAggPlan).assertResults(expected);
}

void testSingleAggregation(
    const std::vector<std::string>& keys,
    const std::string& col,
    const RowVectorPtr& input,
    const RowVectorPtr& expected) {
  auto groupByPlan =
      PlanBuilder()
          .values({input})
          .singleAggregation(keys, {fmt::format("count(distinct {})", col)})
          .planNode();
  AssertQueryBuilder(groupByPlan).assertResults(expected);
}

TEST_F(CountAggregationTest, nans) {
  // Verify that NaNs with different binary representations are considered
  // equal.
  static const auto kNaN = std::numeric_limits<double>::quiet_NaN();
  static const auto kSNaN = std::numeric_limits<double>::signaling_NaN();
  auto data = makeRowVector(
      {makeFlatVector<int16_t>({1, 2, 1, 2, 1, 1, 2, 2}),
       // Column to verify with primitive type input
       makeFlatVector<double>({kNaN, kNaN, kSNaN, kSNaN, 1, 1, 1, 2}),
       // Column to verify with complex type input
       makeRowVector(
           {makeFlatVector<double>({kNaN, kNaN, kSNaN, kSNaN, 1, 1, 1, 2}),
            makeFlatVector<int32_t>({1, 1, 1, 1, 1, 1, 1, 1})})});

  // Global aggregation.
  RowVectorPtr expected = makeRowVector({
      makeFlatVector<int64_t>(std::vector<int64_t>({3})),
  });
  testGlobalAggregation("c1", data, expected);
  testGlobalAggregation("c2", data, expected);

  // Group by.
  expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeFlatVector<int64_t>({2, 3}),
  });
  testSingleAggregation({"c0"}, "c1", data, expected);
  testSingleAggregation({"c0"}, "c2", data, expected);
}

TEST_F(CountAggregationTest, timestampWithTimeZone) {
  // Verify that Timestamps with Time Zones with the same timestamp but
  // different time zones are considered equal.
  auto data = makeRowVector(
      {// Keys non-global for aggregations.
       makeFlatVector<int16_t>({1, 1, 2, 1, 2, 1, 2, 1}),
       // Column to validate aggregating as a stand alone value.
       makeFlatVector<int64_t>(
           {pack(0, 0),
            pack(1, 0),
            pack(2, 0),
            pack(0, 1),
            pack(1, 1),
            pack(1, 2),
            pack(2, 2),
            pack(3, 3)},
           TIMESTAMP_WITH_TIME_ZONE()),
       // Column to validate aggregating as part of a complex value.
       makeRowVector(
           {makeFlatVector<int64_t>(
                {pack(0, 0),
                 pack(1, 0),
                 pack(2, 0),
                 pack(0, 1),
                 pack(1, 1),
                 pack(1, 2),
                 pack(2, 2),
                 pack(3, 3)},
                TIMESTAMP_WITH_TIME_ZONE()),
            makeFlatVector<int32_t>({1, 1, 1, 1, 1, 1, 1, 1})})});

  // Global aggregation.
  RowVectorPtr expected = makeRowVector({
      makeFlatVector<int64_t>(std::vector<int64_t>({4})),
  });
  testGlobalAggregation("c1", data, expected);
  testGlobalAggregation("c2", data, expected);

  // Group by.
  expected = makeRowVector({
      makeFlatVector<int16_t>({1, 2}),
      makeFlatVector<int64_t>({3, 2}),
  });
  testSingleAggregation({"c0"}, "c1", data, expected);
  testSingleAggregation({"c0"}, "c2", data, expected);
}

TEST_F(CountAggregationTest, unknownType) {
  constexpr int kSize = 10;
  auto input = makeRowVector({
      makeFlatVector<int32_t>(kSize, [](auto i) { return i % 2; }),
      makeAllNullFlatVector<UnknownValue>(kSize),
  });
  testGlobalAggregation(
      "c1", input, makeRowVector({makeConstant<int64_t>(0, 1)}));
  testSingleAggregation(
      {"c0"},
      "c1",
      input,
      makeRowVector({
          makeFlatVector<int32_t>({0, 1}),
          makeFlatVector<int64_t>({0, 0}),
      }));
}

} // namespace
} // namespace facebook::velox::aggregate::test
