/*
 *  Copyright 2021 Collate
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.openmetadata.service.openlineage;

import static org.openmetadata.common.utils.CommonUtil.listOrEmpty;
import static org.openmetadata.common.utils.CommonUtil.nullOrEmpty;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.openmetadata.schema.api.lineage.openlineage.InputField;
import org.openmetadata.schema.api.lineage.openlineage.InputFieldTransformation;

/**
 * The {@code function} of a column lineage entry: the expression text OpenLineage reports for its
 * input fields, per ADR:2026-11-23-openlineage-input-transformations-are-the-column-lineage-function.
 */
final class OpenLineageColumnFunction {
  private static final String DESCRIPTION_SEPARATOR = "; ";

  private OpenLineageColumnFunction() {}

  /** The inputs' distinct transformation descriptions, or the transformationDescription. */
  static String of(List<InputField> inputs, String transformationDescription) {
    String described =
        inputs.stream()
            .flatMap(input -> listOrEmpty(input.getTransformations()).stream())
            .filter(Objects::nonNull)
            .map(InputFieldTransformation::getDescription)
            .filter(description -> !nullOrEmpty(description))
            .distinct()
            .collect(Collectors.joining(DESCRIPTION_SEPARATOR));
    return described.isEmpty() ? transformationDescription : described;
  }
}
