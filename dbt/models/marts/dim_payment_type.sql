-- Payment types, codes from the TLC Yellow Taxi data dictionary.
select
    payment_type_id,
    payment_type_name
from {{ ref('payment_type_lookup') }}
