-- Rate codes, codes from the TLC Yellow Taxi data dictionary.
select
    rate_code_id,
    rate_code_name
from {{ ref('rate_code_lookup') }}
