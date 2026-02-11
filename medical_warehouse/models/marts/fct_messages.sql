with stg_messages as (
    select * from {{ ref('stg_telegram_messages') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['channel_title']) }} as channel_key,
    original_message_id,
    message,
    message_date,
    media_path
from stg_messages
