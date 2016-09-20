--drop table jobs cascade;

CREATE TABLE jobs (
	id SERIAL, 
	job_group TEXT, 
	info TEXT, 
	state INT DEFAULT 0, 
	start_processing TIMESTAMP, 
	done_processing TIMESTAMP, 
	error_count INT DEFAULT 0);

CREATE OR replace FUNCTION get_pending_jobs (text, integer) RETURNS SETOF jobs AS
$$
DECLARE
    r jobs % rowtype;
BEGIN
    LOCK TABLE jobs IN EXCLUSIVE MODE;
    FOR r IN
        SELECT * FROM jobs
        WHERE 
        -- unprocessed jobs or in error retry mode
        (state = 0
        OR
        state = -1 AND error_count < 3)
        AND job_group=$1
        ORDER BY id ASC
        LIMIT $2
    LOOP
        UPDATE jobs SET state=1, start_processing=current_timestamp WHERE id=r.id RETURNING * INTO r;
        RETURN NEXT r;
  END LOOP;
  RETURN;
END
$$ LANGUAGE plpgsql VOLATILE STRICT;

CREATE OR replace FUNCTION job_done (integer, integer) RETURNS void AS
$$
DECLARE    
BEGIN
  UPDATE jobs SET state=$2, done_processing=current_timestamp WHERE id=$1;
  RETURN;
END
$$ LANGUAGE plpgsql VOLATILE STRICT;


-- testing

INSERT INTO jobs (job_group, info) VALUES ('news', 'news1');
INSERT INTO jobs (job_group, info) VALUES ('news', 'news2');
INSERT INTO jobs (job_group, info) VALUES ('blog', 'blog1');
INSERT INTO jobs (job_group, info) VALUES ('blog', 'blog2');
INSERT INTO jobs (job_group, info) VALUES ('blog', 'blog3');
INSERT INTO jobs (job_group, info) VALUES ('blog', 'blog4');



select * from jobs;

select get_pending_jobs('news', 1);
select get_pending_jobs('blog', 3);

select * from jobs;
select get_pending_jobs('blog', 3);

select job_done(6, 2);

select * from jobs;

