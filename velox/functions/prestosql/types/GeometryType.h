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

#include <cpl_conv.h>
#include <gdal.h>
#include <gdal_priv.h>
#include <ogr_geometry.h>
#include <stdexcept>
#include "velox/expression/CastExpr.h"
#include "velox/type/Type.h"

namespace facebook::velox {

class GeometryType : public VarbinaryType {
  GeometryType() = default;

 private:
  OGRSpatialReference srs;

 public:
  static const std::shared_ptr<GeometryType>& get() {
    static const std::shared_ptr<GeometryType> instance{new GeometryType()};

    return instance;
  }

  bool equivalent(const Type& other) const override {
    return this == &other;
  }

  const char* name() const override {
    return "GEOMETRY";
  }

  std::string toString() const override {
    return name();
  }

  void handleSRS(OGRGeometry& geometry) {
    if (geometry.getSpatialReference() == nullptr) {
      srs.importFromEPSG(4326);
      geometry.assignSpatialReference(&srs);
    }
  }

  std::string serializeGeometry(OGRGeometry& geometry) {
    handleSRS(geometry);

    int size = geometry.WkbSize();
    std::string wkb(size, 0);

    OGRErr err =
        geometry.exportToWkb(wkbNDR, reinterpret_cast<unsigned char*>(&wkb[0]));
    if (err != OGRERR_NONE) {
      throw std::runtime_error("Failed to export geometry to WKB format");
    }

    return wkb;
  }

  std::shared_ptr<OGRGeometry> deserializeGeometry(const std::string& wkb) {
    if (wkb.empty()) {
      throw std::invalid_argument("WKB string is empty");
    }

    OGRGeometry* geometry = nullptr;
    OGRErr err = OGRGeometryFactory::createFromWkb(
        reinterpret_cast<const unsigned char*>(wkb.data()), nullptr, &geometry);
    if (err != OGRERR_NONE || geometry == nullptr) {
      throw std::runtime_error("Failed to create geometry from WKB");
    }

    return std::shared_ptr<OGRGeometry>(geometry);
  }

  std::string geometryToText(const OGRGeometry* geometry) const {
    if (!geometry) {
      throw std::invalid_argument("Geometry pointer is null");
    }

    char* wkt = nullptr;
    geometry->exportToWkt(&wkt);
    std::string wktStr(wkt);
    CPLFree(wkt);
    return wktStr;
  }

  std::shared_ptr<OGRGeometry> textToGeometry(const std::string& wkt) const {
    if (wkt.empty()) {
      throw std::invalid_argument("WKT string is empty");
    }

    OGRGeometry* geometry = nullptr;
    const char* wktCopy = wkt.c_str();
    OGRErr err =
        OGRGeometryFactory::createFromWkt(&wktCopy, nullptr, &geometry);
    if (err != OGRERR_NONE || geometry == nullptr) {
      throw std::runtime_error("Failed to create geometry from WKT");
    }

    return std::shared_ptr<OGRGeometry>(geometry);
  }
  std::function<std::string(const OGRGeometry*)> castToVartext;
  std::function<std::shared_ptr<OGRGeometry>(const std::string&)>
      castFromVartext;
};

FOLLY_ALWAYS_INLINE bool isGeometryType(const TypePtr& type) {
  return GeometryType::get() == type;
}

FOLLY_ALWAYS_INLINE std::shared_ptr<GeometryType> GEOMETRY() {
  return GeometryType::get();
}

struct GeometryT {
  using type = Varbinary;
  static constexpr const char* typeName = "geometry";
};

using Geometry = CustomType<GeometryT>;

void registerGeometryType();

} // namespace facebook::velox
