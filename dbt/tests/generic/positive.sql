{% test positive(model, column_name) %}
-- Fails on every row where the column is not strictly greater than zero.
select {{ column_name }}
from {{ model }}
where {{ column_name }} <= 0
{% endtest %}
