select
    id,
    node_pool_name,
    nodes_count,
    is_weekend,
    time_cron,
    creation_ts
from nodes_schedule
where time_cron is not null
order by is_weekend, node_pool_name, creation_ts;