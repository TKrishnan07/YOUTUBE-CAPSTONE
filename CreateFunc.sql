CREATE OR REPLACE FUNCTION iso8601_to_seconds(duration TEXT)
RETURNS DOUBLE PRECISION AS $$
DECLARE
    hours_match TEXT[];
    minutes_match TEXT[];
    seconds_match TEXT[];
    hours_int DOUBLE PRECISION := 0;
    minutes_int DOUBLE PRECISION := 0;
    seconds_int DOUBLE PRECISION := 0;
BEGIN
    SELECT regexp_matches(duration, '(\d+)H') INTO hours_match;
    SELECT regexp_matches(duration, '(\d+)M') INTO minutes_match;
    SELECT regexp_matches(duration, '(\d+)S') INTO seconds_match;

    IF hours_match IS NOT NULL THEN
        hours_int := hours_match[1]::DOUBLE PRECISION;
    END IF;

    IF minutes_match IS NOT NULL THEN
        minutes_int := minutes_match[1]::DOUBLE PRECISION;
    END IF;

    IF seconds_match IS NOT NULL THEN
        seconds_int := seconds_match[1]::DOUBLE PRECISION;
    END IF;

    RETURN hours_int * 3600 + minutes_int * 60 + seconds_int;
END;
$$ LANGUAGE plpgsql;
