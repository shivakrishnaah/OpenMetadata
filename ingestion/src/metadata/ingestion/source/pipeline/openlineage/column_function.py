#  Copyright 2025 Collate
#  Licensed under the Collate Community License, Version 1.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  https://github.com/open-metadata/OpenMetadata/blob/main/ingestion/LICENSE
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
The ``function`` of a column lineage entry: the expression text OpenLineage reports for its input
fields, per ADR:2026-11-23-openlineage-input-transformations-are-the-column-lineage-function.
An entry gets the text the server's ``OpenLineageColumnFunction`` gives it; how entries are grouped,
and the REST endpoint's transformationDescription fallback, differ as that record states.
"""

DESCRIPTION_SEPARATOR = "; "


def column_function(input_fields: list[dict]) -> str | None:
    """The input fields' distinct transformation descriptions, or None when none has one."""
    descriptions: list[str] = []
    for input_field in input_fields:
        transformations = input_field.get("transformations")
        for transformation in transformations if isinstance(transformations, list) else []:
            description = transformation.get("description") if isinstance(transformation, dict) else None
            if isinstance(description, str) and description and description not in descriptions:
                descriptions.append(description)
    return DESCRIPTION_SEPARATOR.join(descriptions) or None
