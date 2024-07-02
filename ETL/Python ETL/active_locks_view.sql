-- A convenient view to monitor any locking processes in postgres db instance

-- returns all active locks
-- SELECT * FROM public.active_locks;

-- Deletes an specific lock based on the process id affected
-- SELECT pg_terminate_backend(%pid%)

CREATE OR REPLACE VIEW public.active_locks AS
	
SELECT t.schemaname, t.relname, l.locktype, l.page, l.virtualtransaction, l.pid, l.mode, l.granted

	FROM pg_locks l

	JOIN pg_stat_all_tables t ON l.relation = t.relid

	WHERE t.schemaname <> 'pg_toast'::name AND t.schemaname <> 'pg_catalog'::name

	ORDER BY t.schemaname, t.relname;