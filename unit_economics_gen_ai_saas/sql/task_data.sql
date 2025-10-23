select
    th.task_id,
    th.status,
    th.creation_ts,
    t.metainfo,
    t.node_id,
    n.service_name,
    date(t.creation_ts) as task_date,
    extract (DOW from t.creation_ts) in (0, 6) as is_weekend
from task_history th
join tasks t on th.task_id = t.id
left join nodes n on t.node_id = n.id
where t.creation_ts between '2099-06-01 00:00:00' and '2099-08-31 23:59:59'
  and t.metainfo is not null
  and th.status not in ('new', 'waiting', 'pending')
  and n.service_name not ilike '%%api%%'
order by th.task_id, th.creation_ts;