import re

with open('tests/test_resolve_columns_empty_string.py', 'r') as f:
    content = f.read()

# Replace mock_df.withColumn.assert_called_with with mock_df.withColumns.assert_called_with
# because the code uses withColumns now
content = content.replace('mock_df.withColumn.assert_called_with("mycol", "FINAL_COALESCE")', 'mock_df.withColumns.assert_called_with({"mycol": "FINAL_COALESCE"})')
content = content.replace('mock_df.withColumn.return_value = mock_df', 'mock_df.withColumns.return_value = mock_df')

with open('tests/test_resolve_columns_empty_string.py', 'w') as f:
    f.write(content)
