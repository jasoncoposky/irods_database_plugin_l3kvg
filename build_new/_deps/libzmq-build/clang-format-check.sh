#!/bin/sh
FAILED=0
IFS=";"
FILES="../../../src/catalog/catalog_facade.cpp;../../../src/compiler/gq2_compiler.cpp;../../../src/db_plugin.cpp;../../../tests/mock_l3kvg.hpp;../../../tests/plugin_test_fixture.hpp;../../../tests/test_admin_queries.cpp;../../../tests/test_advanced_operators.cpp;../../../tests/test_aggregates.cpp;../../../tests/test_collections.cpp;../../../tests/test_deep_traversal.cpp;../../../tests/test_distinct.cpp;../../../tests/test_gq2_metadata_compiler.cpp;../../../tests/test_groupby.cpp;../../../tests/test_identity.cpp;../../../tests/test_legacy_compatibility.cpp;../../../tests/test_metadata_acls.cpp;../../../tests/test_multihop_query.cpp;../../../tests/test_nested_logic.cpp;../../../tests/test_plugin_acls.cpp;../../../tests/test_plugin_collections.cpp;../../../tests/test_plugin_data.cpp;../../../tests/test_plugin_federation.cpp;../../../tests/test_plugin_identity.cpp;../../../tests/test_plugin_metadata.cpp;../../../tests/test_plugin_misc.cpp;../../../tests/test_plugin_resources.cpp;../../../tests/test_sorting.cpp"
IDS=$(echo -en "\n\b")
for FILE in $FILES
do
	clang-format -style=file -output-replacements-xml "$FILE" | grep "<replacement " >/dev/null &&
    {
      echo "$FILE is not correctly formatted"
	  FAILED=1
	}
done
if [ "$FAILED" -eq "1" ] ; then exit 1 ; fi
