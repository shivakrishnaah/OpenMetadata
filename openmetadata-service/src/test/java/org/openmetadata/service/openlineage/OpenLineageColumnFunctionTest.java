package org.openmetadata.service.openlineage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.openmetadata.schema.api.lineage.openlineage.InputField;
import org.openmetadata.schema.api.lineage.openlineage.InputFieldTransformation;

class OpenLineageColumnFunctionTest {

  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void writesTheInputsDescriptions(
      String scenario, List<InputField> inputs, String transformationDescription, String expected) {
    assertEquals(expected, OpenLineageColumnFunction.of(inputs, transformationDescription));
  }

  static Stream<Arguments> cases() {
    return Stream.of(
        Arguments.of(
            "one description",
            List.of(field("amount", described("sum((amount * rate))"))),
            null,
            "sum((amount * rate))"),
        Arguments.of(
            "inputs with the same description",
            List.of(
                field("amount", described("sum((amount * rate))")),
                field("rate", described("sum((amount * rate))"))),
            null,
            "sum((amount * rate))"),
        Arguments.of(
            "different descriptions are all kept",
            List.of(
                field("amount", described("sum((amount * rate))")),
                field("region", described("(region = 'EU')"))),
            null,
            "sum((amount * rate)); (region = 'EU')"),
        Arguments.of(
            "an input's several transformations",
            List.of(
                field("ccy", described("INNER JOIN ON (ccy = ccy)"), described("isnotnull(ccy)"))),
            null,
            "INNER JOIN ON (ccy = ccy); isnotnull(ccy)"),
        Arguments.of(
            "a type without a description is not text",
            List.of(field("amount", new InputFieldTransformation().withType("DIRECT"))),
            null,
            null),
        Arguments.of(
            "empty and null entries are skipped",
            List.of(
                field("amount", new InputFieldTransformation().withDescription(""), null),
                field("rate", described("sum((amount * rate))"))),
            null,
            "sum((amount * rate))"),
        Arguments.of(
            "descriptions win over the transformationDescription",
            List.of(field("amount", described("sum((amount * rate))"))),
            "IDENTITY",
            "sum((amount * rate))"),
        Arguments.of(
            "without descriptions the transformationDescription is kept",
            List.of(field("source_col")),
            "IDENTITY",
            "IDENTITY"),
        Arguments.of(
            "without either there is no function", List.of(field("source_col")), null, null));
  }

  private static InputField field(String name, InputFieldTransformation... transformations) {
    return new InputField()
        .withNamespace("ns")
        .withName("schema.table")
        .withField(name)
        .withTransformations(Arrays.asList(transformations));
  }

  private static InputFieldTransformation described(String description) {
    return new InputFieldTransformation().withType("DIRECT").withDescription(description);
  }
}
