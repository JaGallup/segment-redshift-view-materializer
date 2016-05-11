# segment-redshift-view-materializer
Set of Python scripts to iterate over all your schemas created by Segment Warehouse and create "materialized views" (actually `DROP TABLE` and `CREATE TABLE` calls). Meant to be used with a cron job or AWS Lambda to `DROP` and `CREATE` on a scheduled basis.

Tables created:

1. `{schema}.event_union`: Unions the `pages` and `tracks` tables, and calculates idle time between actions in both minutes and seconds. Checks for any column names that begin with `context_` and adds those to the union, as well as checking for `user_id`, `name`, and `category`. Need to figure out how to check for additional properties that might have been used.

2. `{schema}.sessions`: Creates session IDs based on `anonymous_id` and `idle_time_minutes`. Right now hard-coded for `idle_time_minutes > 30`. In future will need to figure out how to pass through whether to use `user_id` or `anonymous_id` and to change value to use for `idle_time_minutes`.

3. `{schema}.event_facts`: Combines information from the previous two tables to create a facts table to query against. Computes `track_sequence_number` (i.e., "This is the Xth action this session") as well as `source_sequence_number` (i.e., "This is the Nth pageview/event this session").
