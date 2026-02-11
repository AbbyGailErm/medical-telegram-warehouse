with source as (
    select * from {{ source('raw', 'telegram_messages') }}
),

cleaned as (
    select
        id as original_message_id,
        channel_title,
        channel_username,
        message,
        date::timestamp as message_date,
        media_path
    from source
    where message is not null or media_path is not null
)

select * from cleaned
