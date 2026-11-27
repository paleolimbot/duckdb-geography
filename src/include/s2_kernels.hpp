#pragma once

#include "s2geography/sedona_udf/sedona_extension.h"

namespace s2geography {
namespace sedona_udf {

void LengthKernel(struct SedonaCScalarKernel* out);
void AreaKernel(struct SedonaCScalarKernel* out);
void PerimeterKernel(struct SedonaCScalarKernel* out);

void DistanceKernel(struct SedonaCScalarKernel* out, bool prepare_arg0_scalar = true,
                    bool prepare_arg1_scalar = true);
void MaxDistanceKernel(struct SedonaCScalarKernel* out, bool prepare_arg0_scalar = true,
                       bool prepare_arg1_scalar = true);

void DifferenceKernel(struct SedonaCScalarKernel* out);
void IntersectionKernel(struct SedonaCScalarKernel* out);
void UnionKernel(struct SedonaCScalarKernel* out);

void CellIdFromPointKernel(struct SedonaCScalarKernel* out);
void CoveringCellIdsKernel(struct SedonaCScalarKernel* out);
void CoveringCellIdsLevelRangeKernel(struct SedonaCScalarKernel* out);
void BoundingBoxKernel(struct SedonaCScalarKernel* out);

}  // namespace sedona_udf
}  // namespace s2geography
