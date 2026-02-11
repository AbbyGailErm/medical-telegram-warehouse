with stg_messages as (
    select * from {{ ref('stg_telegram_messages') }}
)

select distinct
    {{ dbt_utils.generate_surrogate_key(['channel_title']) }} as channel_key,
    channel_title,
    channel_username
from stg_messages
