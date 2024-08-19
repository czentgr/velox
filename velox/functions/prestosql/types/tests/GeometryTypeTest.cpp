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
#include "velox/functions/prestosql/types/GeometryType.h"
#include <gdal.h>
#include <ogr_geometry.h>
#include "velox/functions/prestosql/types/tests/TypeTestBase.h"

namespace facebook::velox::test {

class GeometryTypeTest : public testing::Test, public TypeTestBase {
 public:
  GeometryTypeTest() {
    registerGeometryType();
  }
};

TEST_F(GeometryTypeTest, basic) {
  ASSERT_EQ(GEOMETRY()->name(), "GEOMETRY");
  ASSERT_EQ(GEOMETRY()->kindName(), "VARBINARY");
  ASSERT_TRUE(GEOMETRY()->parameters().empty());
  ASSERT_EQ(GEOMETRY()->toString(), "GEOMETRY");

  // Verify that the type is correctly registered
  ASSERT_TRUE(hasType("GEOMETRY"));
  ASSERT_EQ(*getType("GEOMETRY", {}), *GEOMETRY());
}

TEST_F(GeometryTypeTest, serializationDeserializationPointWithCasting) {
  OGRPoint point(1.0, 2.0);

  std::string wkb = GEOMETRY()->serializeGeometry(point);

  auto deserializedGeometry = GEOMETRY()->deserializeGeometry(wkb);

  ASSERT_TRUE(deserializedGeometry != nullptr);
  ASSERT_EQ(deserializedGeometry->getGeometryType(), wkbPoint);

  char* wkt = nullptr;
  deserializedGeometry->exportToWkt(&wkt);
  ASSERT_EQ(std::string(wkt), "POINT (1 2)");

  std::string wktFromCast =
      GEOMETRY()->castToVartext(deserializedGeometry.get());
  ASSERT_EQ(wktFromCast, "POINT (1 2)");

  auto castedGeometry = GEOMETRY()->castFromVartext(wktFromCast);
  ASSERT_TRUE(castedGeometry != nullptr);
  ASSERT_EQ(castedGeometry->getGeometryType(), wkbPoint);

  char* castedWkt = nullptr;
  castedGeometry->exportToWkt(&castedWkt);
  ASSERT_EQ(std::string(castedWkt), "POINT (1 2)");
}

TEST_F(GeometryTypeTest, serializationDeserializationPolygon) {
  OGRPolygon polygon;
  OGRLinearRing ring;
  ring.addPoint(0.0, 0.0);
  ring.addPoint(1.0, 0.0);
  ring.addPoint(1.0, 1.0);
  ring.addPoint(0.0, 1.0);
  ring.addPoint(0.0, 0.0);
  polygon.addRing(&ring);

  std::string wkb = GEOMETRY()->serializeGeometry(polygon);

  auto deserializedGeometry = GEOMETRY()->deserializeGeometry(wkb);

  ASSERT_TRUE(deserializedGeometry != nullptr);
  ASSERT_EQ(deserializedGeometry->getGeometryType(), wkbPolygon);

  char* wkt = nullptr;
  deserializedGeometry->exportToWkt(&wkt);
  ASSERT_EQ(std::string(wkt), "POLYGON ((0 0,1 0,1 1,0 1,0 0))");

  std::string wktFromCast =
      GEOMETRY()->castToVartext(deserializedGeometry.get());
  ASSERT_EQ(wktFromCast, "POLYGON ((0 0,1 0,1 1,0 1,0 0))");

  auto castedGeometry = GEOMETRY()->castFromVartext(wktFromCast);
  ASSERT_TRUE(castedGeometry != nullptr);
  ASSERT_EQ(castedGeometry->getGeometryType(), wkbPolygon);

  char* castedWkt = nullptr;
  castedGeometry->exportToWkt(&castedWkt);
  ASSERT_EQ(std::string(castedWkt), "POLYGON ((0 0,1 0,1 1,0 1,0 0))");
}

TEST_F(GeometryTypeTest, serializationDeserializationMultiGeometry) {
  OGRMultiPoint multiPoint;
  multiPoint.addGeometry(new OGRPoint(1.0, 2.0));
  multiPoint.addGeometry(new OGRPoint(3.0, 4.0));

  std::string wkb = GEOMETRY()->serializeGeometry(multiPoint);

  auto deserializedGeometry = GEOMETRY()->deserializeGeometry(wkb);

  ASSERT_TRUE(deserializedGeometry != nullptr);
  ASSERT_EQ(deserializedGeometry->getGeometryType(), wkbMultiPoint);

  char* wkt = nullptr;
  deserializedGeometry->exportToWkt(&wkt);
  ASSERT_EQ(std::string(wkt), "MULTIPOINT (1 2,3 4)");

  std::string wktFromCast =
      GEOMETRY()->castToVartext(deserializedGeometry.get());
  ASSERT_EQ(wktFromCast, "MULTIPOINT (1 2,3 4)");

  auto castedGeometry = GEOMETRY()->castFromVartext(wktFromCast);
  ASSERT_TRUE(castedGeometry != nullptr);
  ASSERT_EQ(castedGeometry->getGeometryType(), wkbMultiPoint);

  char* castedWkt = nullptr;
  castedGeometry->exportToWkt(&castedWkt);
  ASSERT_EQ(std::string(castedWkt), "MULTIPOINT (1 2,3 4)");
}

TEST_F(GeometryTypeTest, handleInvalidGeometry) {
  std::string invalidWkb = "invalid_wkb";

  try {
    auto deserializedGeometry = GEOMETRY()->deserializeGeometry(invalidWkb);
    FAIL() << "Expected std::runtime_error due to invalid WKB";
  } catch (const std::runtime_error& err) {
    ASSERT_STREQ(err.what(), "Failed to create geometry from WKB");
  } catch (...) {
    FAIL() << "Expected std::runtime_error";
  }
}

} // namespace facebook::velox::test
